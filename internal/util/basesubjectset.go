package util

import (
	"golang.org/x/exp/maps"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Subject is a subject that can be placed into a BaseSubjectSet. It is defined in a generic
// manner to allow implementations that wrap BaseSubjectSet to add their own additional bookkeeping
// to the base implementation.
type Subject[T any] interface {
	// GetSubjectId returns the ID of the subject. For wildcards, this should be `*`.
	GetSubjectId() string

	// GetCaveatExpression returns the caveat expression for this subject, if it is conditional.
	GetCaveatExpression() *v1.CaveatExpression

	// GetExcludedSubjects returns the list of subjects excluded. Must only have values
	// for wildcards and must never be nested.
	GetExcludedSubjects() []T
}

// BaseSubjectSet defines a set that tracks accessible subjects, their exclusions (if wildcards),
// and all conditional expressions applied due to caveats.
//
// It is generic to allow other implementations to define the kind of tracking information
// associated with each subject.
//
// NOTE: Unlike a traditional set, unions between wildcards and a concrete subject will result
// in *both* being present in the set, to maintain the proper set semantics around wildcards.
type BaseSubjectSet[T Subject[T]] struct {
	concrete *concreteSubjectSet[T]
	wildcard *wildcardSubjectTracker[T]
}

// NewBaseSubjectSet creates a new base subject set for use underneath well-typed implementation.
//
// The constructor function returns a new instance of type T for a particular subject ID.
func NewBaseSubjectSet[T Subject[T]](constructor constructor[T]) BaseSubjectSet[T] {
	return BaseSubjectSet[T]{
		concrete: newConcreteSubjectSet[T](constructor),
		wildcard: newWildcardSubjectTracker[T](constructor),
	}
}

// constructor defines a function for constructing a new instance of the Subject type T for
// a subject ID, its (optional) conditional expression, any excluded subjects, and any sources
// for bookkeeping. The sources are those other subjects that were combined to create the current
// subject.
type constructor[T Subject[T]] func(subjectID string, conditionalExpression *v1.CaveatExpression, excludedSubjects []T, sources ...T) T

// Add adds the found subject to the set. This is equivalent to a Union operation between the
// existing set of subjects and a set containing the single subject, but modifies the set
// *in place*.
func (bss BaseSubjectSet[T]) Add(foundSubject T) {
	if foundSubject.GetSubjectId() == tuple.PublicWildcard {
		bss.wildcard.unionWildcard(foundSubject)

		// Union each concrete into the wildcard, to ensure exclusions are handled.
		for _, concrete := range bss.concrete.subjects {
			bss.wildcard.unionConcrete(concrete)
		}
		return
	}

	if len(foundSubject.GetExcludedSubjects()) > 0 {
		panic("found excluded subjects for non-wildcard")
	}

	bss.wildcard.unionConcrete(foundSubject)
	bss.concrete.unionConcrete(foundSubject)
}

// Subtract subtracts the given subject found the set.
func (bss BaseSubjectSet[T]) Subtract(foundSubject T) {
	if foundSubject.GetSubjectId() == tuple.PublicWildcard {
		bss.concrete.subtractWildcard(foundSubject)

		// NOTE: subtracting a wildcard from another wildcard can result in concrete subjects
		// being produced, which are added here, if any.
		for _, subject := range bss.wildcard.subtractWildcard(foundSubject) {
			bss.concrete.unionConcrete(subject)
		}
		return
	}

	if len(foundSubject.GetExcludedSubjects()) > 0 {
		panic("found excluded subjects for non-wildcard")
	}

	bss.wildcard.subtractConcrete(foundSubject)
	bss.concrete.subtractConcrete(foundSubject)
}

// SubtractAll subtracts the other set of subjects from this set of subtracts, modifying this
// set *in place*.
func (bss BaseSubjectSet[T]) SubtractAll(other BaseSubjectSet[T]) {
	if wildcard, ok := other.wildcard.get(); ok {
		bss.Subtract(wildcard)
	}

	for _, concrete := range other.concrete.subjects {
		bss.Subtract(concrete)
	}
}

// IntersectionDifference performs an intersection between this set and the other set, modifying
// this set *in place*.
func (bss BaseSubjectSet[T]) IntersectionDifference(other BaseSubjectSet[T]) {
	// Process the intersection between the *concrete* subjects.
	otherWildcard, hasOtherWildcard := other.wildcard.get()
	otherWildcardExclusionsMap := other.wildcard.exclusionsMap()

	for _, currentConcreteSubject := range bss.concrete.subjects {
		// Check if the other set either has a concrete or a wildcard. If not, remove this entry.
		otherConcreteSubject, isConcreteInOtherSite := other.concrete.subjects[currentConcreteSubject.GetSubjectId()]
		if isConcreteInOtherSite {
			// And together to handle conditionals and sourcing.
			bss.concrete.subjects[currentConcreteSubject.GetSubjectId()] = bss.concrete.constructor(
				currentConcreteSubject.GetSubjectId(),
				caveatAnd(currentConcreteSubject.GetCaveatExpression(), otherConcreteSubject.GetCaveatExpression()),
				nil,
				currentConcreteSubject,
				otherConcreteSubject)
			continue
		}

		if !hasOtherWildcard {
			delete(bss.concrete.subjects, currentConcreteSubject.GetSubjectId())
			continue
		}

		// Check if the concrete subject is excluded or updated.
		otherExclusion, isExcluded := otherWildcardExclusionsMap[currentConcreteSubject.GetSubjectId()]
		updatedConcrete, remainsConcrete := bss.intersectConcreteAndWildcard(currentConcreteSubject, otherWildcard, otherExclusion, isExcluded)
		if !remainsConcrete {
			delete(bss.concrete.subjects, currentConcreteSubject.GetSubjectId())
			continue
		}

		bss.concrete.subjects[currentConcreteSubject.GetSubjectId()] = updatedConcrete
	}

	// If the current set has a wildcard, add any concrete subjects from the other set that are
	// not in the wildcard's exclusion set, and are not in the current set's concrete set.
	if currentWildcard, hasCurrentWildcard := bss.wildcard.get(); hasCurrentWildcard {
		currentWildcardExclusionsMap := bss.wildcard.exclusionsMap()
		for _, otherConcreteSubject := range other.concrete.subjects {
			_, isInCurrentSet := bss.concrete.subjects[otherConcreteSubject.GetSubjectId()]
			if isInCurrentSet {
				// Already present.
				continue
			}

			// Otherwise, add to the concrete set if not in the exclusions of the wildcard or if the
			// exclusion is conditional.
			exclusion, isExcluded := currentWildcardExclusionsMap[otherConcreteSubject.GetSubjectId()]
			updatedConcrete, remainsConcrete := bss.intersectConcreteAndWildcard(otherConcreteSubject, currentWildcard, exclusion, isExcluded)
			if !remainsConcrete {
				// Nothing to do, since already removed from the set.
				continue
			}

			bss.concrete.subjects[updatedConcrete.GetSubjectId()] = updatedConcrete
		}
	}

	// Update the wildcard.
	bss.wildcard.intersectDiffWithWildcard(other.wildcard)
}

func (bss BaseSubjectSet[T]) intersectConcreteAndWildcard(concrete T, wildcard T, exclusion T, isExcluded bool) (T, bool) {
	// Cases:
	// - The concrete subject is not excluded and the wildcard is not conditional => concrete is kept
	// - The concrete subject is excluded and the wildcard is not conditional => concrete is removed
	// - The concrete subject is not excluded but the wildcard is conditional => concrete is kept, but made conditional
	// - The concrete subject is excluded and the wildcard is conditional => concrete is removed, since it is always excluded
	// - The concrete subject is excluded and the wildcard is conditional and the exclusion is conditional => combined conditional
	// - The concrete subject is excluded and the wildcard is not conditional but the exclusion *is* conditional => concrete is made conditional
	switch {
	case !isExcluded && wildcard.GetCaveatExpression() == nil:
		// If the concrete is not excluded and the wildcard conditional is empty, then the concrete is always found.
		// Example: {user:tom} & {*} => {user:tom}
		return concrete, true

	case isExcluded && wildcard.GetCaveatExpression() == nil:
		// If the concrete is excluded and the wildcard conditional is empty, then the concrete is never found.
		// Example: {user:tom} & {* - user:tom} => {}
		return concrete, false

	case !isExcluded && wildcard.GetCaveatExpression() != nil:
		// The concrete subject is only included if the wildcard's caveat is true.
		// Example: {user:tom}[acaveat] & {* - user:tom}[somecaveat] => {user:tom}[acaveat && somecaveat]
		return bss.concrete.constructor(
				concrete.GetSubjectId(),
				caveatAnd(concrete.GetCaveatExpression(), wildcard.GetCaveatExpression()),
				nil,
				concrete,
				wildcard),
			true

	case isExcluded && exclusion.GetCaveatExpression() == nil:
		// If the concrete is excluded and the exclusion is not conditional, then the concrete can never show up,
		// regardless of whether the wildcard is conditional.
		// Example: {user:tom} & {* - user:tom}[somecaveat] => {}
		return concrete, false

	case isExcluded && exclusion.GetCaveatExpression() != nil:
		// NOTE: whether the wildcard is itself conditional or not is handled within the expression combinators below.
		// The concrete subject is included if the wildcard's caveat is true and the exclusion's caveat is *false*.
		// Example: {user:tom}[acaveat] & {* - user:tom[ecaveat]}[wcaveat] => {user:tom[acaveat && wcaveat && !ecaveat]}
		return bss.concrete.constructor(
				concrete.GetSubjectId(),
				caveatAnd(
					concrete.GetCaveatExpression(),
					caveatAnd(
						wildcard.GetCaveatExpression(),
						caveatInvert(exclusion.GetCaveatExpression()),
					)),
				nil,
				concrete,
				wildcard,
				exclusion),
			true
	}

	return *new(T), false
}

// UnionWith adds the given subjects to this set, via a union call.
func (bss BaseSubjectSet[T]) UnionWith(foundSubjects []T) {
	for _, fs := range foundSubjects {
		bss.Add(fs)
	}
}

// UnionWithSet performs a union operation between this set and the other set, modifying this
// set in place.
func (bss BaseSubjectSet[T]) UnionWithSet(other BaseSubjectSet[T]) {
	bss.UnionWith(other.AsSlice())
}

// Get returns the found subject with the given ID in the set, if any.
func (bss BaseSubjectSet[T]) Get(id string) (T, bool) {
	if id == tuple.PublicWildcard {
		return bss.wildcard.get()
	}

	return bss.concrete.get(id)
}

// IsEmpty returns whether the subject set is empty.
func (bss BaseSubjectSet[T]) IsEmpty() bool {
	return bss.concrete.isEmpty() && !bss.wildcard.has()
}

// AsSlice returns the contents of the subject set as a slice of found subjects.
func (bss BaseSubjectSet[T]) AsSlice() []T {
	return append(bss.concrete.asSlice(), bss.wildcard.asSlice()...)
}

// Clone returns a clone of this subject set. Note that this is a shallow clone.
// NOTE: Should only be used when performance is not a concern.
func (bss BaseSubjectSet[T]) Clone() BaseSubjectSet[T] {
	return BaseSubjectSet[T]{
		bss.concrete.clone(),
		bss.wildcard.clone(),
	}
}

// UnsafeRemoveExact removes the *exact* matching subject, with no wildcard handling.
// This should ONLY be used for testing.
func (bss BaseSubjectSet[T]) UnsafeRemoveExact(foundSubject T) {
	if foundSubject.GetSubjectId() == tuple.PublicWildcard {
		bss.wildcard.wildcard = nil
		return
	}

	delete(bss.concrete.subjects, foundSubject.GetSubjectId())
}

// concreteSubjectSet is a subject set which tracks concrete subjects only.
type concreteSubjectSet[T Subject[T]] struct {
	constructor constructor[T]

	// subjects is the map from subject ID of each concrete subject in the set to its associated
	// Subject.
	subjects map[string]T
}

func newConcreteSubjectSet[T Subject[T]](constructor constructor[T]) *concreteSubjectSet[T] {
	if constructor == nil {
		panic("given nil constructor")
	}

	return &concreteSubjectSet[T]{
		subjects:    map[string]T{},
		constructor: constructor,
	}
}

func (cst *concreteSubjectSet[T]) get(subjectID string) (T, bool) {
	found, ok := cst.subjects[subjectID]
	return found, ok
}

func (cst *concreteSubjectSet[T]) isEmpty() bool {
	return len(cst.subjects) == 0
}

func (cst *concreteSubjectSet[T]) asSlice() []T {
	return maps.Values(cst.subjects)
}

func (cst *concreteSubjectSet[T]) clone() *concreteSubjectSet[T] {
	return &concreteSubjectSet[T]{
		constructor: cst.constructor,
		subjects:    maps.Clone(cst.subjects),
	}
}

func (cst *concreteSubjectSet[T]) unionConcrete(concrete T) {
	// A union of a concrete subject needs to check if the existing concrete subject is present.
	// Cases:
	// - If not present => the new concrete subject is used as-is.
	// - If already present => the conditionals of each concrete need to be merged.
	existing, ok := cst.subjects[concrete.GetSubjectId()]
	if !ok {
		cst.subjects[concrete.GetSubjectId()] = concrete
		return
	}

	cst.subjects[concrete.GetSubjectId()] = cst.constructor(
		concrete.GetSubjectId(),
		shortcircuitedOr(existing.GetCaveatExpression(), concrete.GetCaveatExpression()),
		nil,
		concrete, existing)
}

func (cst *concreteSubjectSet[T]) subtractWildcard(wildcard T) {
	// Subtraction of a wildcard removes *all* elements of the concrete set, except those that
	// are found in the excluded list. If the wildcard *itself* is conditional, then instead of
	// items being removed, they are made conditional on the inversion of the wildcard's expression,
	// and the exclusion's conditional, if any.
	//
	// Examples:
	//  {user:sarah, user:tom} - {*} => {}
	//  {user:sarah, user:tom} - {*[somecaveat]} => {user:sarah[!somecaveat], user:tom[!somecaveat]}
	//  {user:sarah, user:tom} - {* - {user:tom}} => {user:tom}
	//  {user:sarah, user:tom} - {*[somecaveat] - {user:tom}} => {user:sarah[!somecaveat], user:tom}
	//  {user:sarah, user:tom} - {* - {user:tom[c2]}}[somecaveat] => {user:sarah[!somecaveat], user:tom[c2]}
	//  {user:sarah[c1], user:tom} - {*[somecaveat] - {user:tom}} => {user:sarah[c1 && !somecaveat], user:tom}
	exclusions := exclusionsMapFor(wildcard)
	for existingSubjectID, existingSubject := range cst.subjects {
		exclusion, isExcluded := exclusions[existingSubjectID]
		if !isExcluded {
			// If the subject was not excluded within the wildcard, it is either removed directly
			// (in the case where the wildcard is not conditional), or has its condition updated to
			// reflect that it is only present when the condition for the wildcard is *false*.
			if wildcard.GetCaveatExpression() == nil {
				delete(cst.subjects, existingSubjectID)
				continue
			}

			cst.subjects[existingSubjectID] = cst.constructor(
				existingSubjectID,
				caveatAnd(existingSubject.GetCaveatExpression(), caveatInvert(wildcard.GetCaveatExpression())),
				nil,
				existingSubject)
			continue
		}

		// If the exclusion is not conditional, then the subject is always present.
		if exclusion.GetCaveatExpression() == nil {
			continue
		}

		// The conditional of the exclusion is that of the exclusion itself OR the caveatInverted case of
		// the wildcard, which would mean the wildcard itself does not apply.
		exclusionConditional := caveatOr(caveatInvert(wildcard.GetCaveatExpression()), exclusion.GetCaveatExpression())

		// If the whole exclusion is not conditional, nothing more to do.
		if exclusionConditional == nil {
			continue
		}

		// Otherwise, add the exclusion's conditional to the entry.
		cst.subjects[existingSubjectID] = cst.constructor(
			existingSubjectID,
			caveatAnd(existingSubject.GetCaveatExpression(), exclusionConditional),
			nil,
			existingSubject)
	}
}

func (cst *concreteSubjectSet[T]) subtractConcrete(concrete T) {
	existing, ok := cst.subjects[concrete.GetSubjectId()]
	if !ok {
		// If not present, nothing more to do.
		return
	}

	// Subtraction of a concrete type removes the entry from the concrete list
	// *unless* the subtraction is conditional, in which case the conditional is updated
	// to remove the element when it is true.
	//
	// Examples:
	//  {user:sarah} - {user:tom} => {user:sarah}
	//  {user:tom} - {user:tom} => {}
	//  {user:tom[c1]} - {user:tom} => {user:tom}
	//  {user:tom} - {user:tom[c2]} => {user:tom[!c2]}
	//  {user:tom[c1]} - {user:tom[c2]} => {user:tom[c1 && !c2]}
	if concrete.GetCaveatExpression() == nil {
		delete(cst.subjects, concrete.GetSubjectId())
		return
	}

	// Otherwise, adjust the conditional of the existing item to remove it if it is true.
	cst.subjects[concrete.GetSubjectId()] = cst.constructor(
		concrete.GetSubjectId(),
		caveatAnd(existing.GetCaveatExpression(), caveatInvert(concrete.GetCaveatExpression())),
		nil,
		concrete, existing)
}

func (cst *concreteSubjectSet[T]) intersectDiffWithConcrete(other *concreteSubjectSet[T]) {
	// Intersection of concrete subjects sets is a standard intersection operation, where elements
	// must be in both sets, with a combination of the two elements into one for conditionals.
	for currentSubjectID, existingSubject := range cst.subjects {
		// Remove any entries not found in both subject sets, and combine their conditionals.
		otherSubject, ok := other.subjects[currentSubjectID]
		if !ok {
			delete(cst.subjects, currentSubjectID)
			continue
		}

		// Otherwise, `and` together conditionals.
		cst.subjects[currentSubjectID] = cst.constructor(
			currentSubjectID,
			caveatAnd(existingSubject.GetCaveatExpression(), otherSubject.GetCaveatExpression()),
			nil,
			existingSubject,
			otherSubject)
	}
}

// wildcardSubjectTracker tracks the wildcard found in a subject set, if any.
type wildcardSubjectTracker[T Subject[T]] struct {
	constructor func(subjectID string, conditionalExpression *v1.CaveatExpression, excludedSubjects []T, sources ...T) T
	wildcard    *T
}

func newWildcardSubjectTracker[T Subject[T]](constructor constructor[T]) *wildcardSubjectTracker[T] {
	if constructor == nil {
		panic("given nil constructor")
	}

	return &wildcardSubjectTracker[T]{constructor: constructor, wildcard: nil}
}

func (wst *wildcardSubjectTracker[T]) get() (T, bool) {
	if wst.wildcard != nil {
		return *wst.wildcard, true
	}

	return *new(T), false
}

func (wst *wildcardSubjectTracker[T]) has() bool {
	return wst.wildcard != nil
}

func (wst *wildcardSubjectTracker[T]) asSlice() []T {
	if wst.wildcard == nil {
		return []T{}
	}

	return []T{*wst.wildcard}
}

func (wst *wildcardSubjectTracker[T]) clone() *wildcardSubjectTracker[T] {
	return &wildcardSubjectTracker[T]{
		constructor: wst.constructor,
		wildcard:    wst.wildcard,
	}
}

func (wst *wildcardSubjectTracker[T]) unionWildcard(foundWildcard T) {
	// If there is no existing wildcard, use the one that was found.
	if wst.wildcard == nil {
		wst.wildcard = &foundWildcard
		return
	}

	// Otherwise, union together the conditionals for the wildcards and *intersect* their exclusion
	// sets. Exclusion sets are intersected because if an exclusion is missing from one wildcard
	// but not the other, the missing element will be, by definition, in that other wildcard.
	//
	// Examples:
	//  {*} + {*} => {*}
	//  {* - {user:tom}} + {*} => {*}
	//  {* - {user:tom}} + {* - {user:sarah}} => {*}
	//  {* - {user:tom, user:sarah}} + {* - {user:sarah}} => {* - {user:sarah}}
	//  {*}[c1] + {*} => {*}
	//  {*}[c1] + {*}[c2] => {*}[c1 || c2]
	existing := *wst.wildcard

	exisingConcreteExclusions := newConcreteSubjectSet[T](wst.constructor)
	for _, excludedSubject := range existing.GetExcludedSubjects() {
		exisingConcreteExclusions.unionConcrete(excludedSubject)
	}

	foundConcreteExclusions := newConcreteSubjectSet[T](wst.constructor)
	for _, excludedSubject := range foundWildcard.GetExcludedSubjects() {
		foundConcreteExclusions.unionConcrete(excludedSubject)
	}

	exisingConcreteExclusions.intersectDiffWithConcrete(foundConcreteExclusions)

	constructed := wst.constructor(
		tuple.PublicWildcard,
		shortcircuitedOr(existing.GetCaveatExpression(), foundWildcard.GetCaveatExpression()),
		exisingConcreteExclusions.asSlice(),
		existing,
		foundWildcard)
	wst.wildcard = &constructed
}

func (wst *wildcardSubjectTracker[T]) unionConcrete(concrete T) {
	if wst.wildcard == nil {
		// Nothing to do if no wildcard present.
		return
	}

	// If the concrete is in the exclusion set, remove it if not conditional. Otherwise, mark
	// it as conditional.
	//
	// Examples:
	//  {*} | {user:tom} => {*} (and user:tom in the concrete)
	//  {* - {user:tom}} | {user:tom} => {*} (and user:tom in the concrete)
	//  {* - {user:tom}[c1]} | {user:tom}[c2] => {* - {user:tom}[c1 && !c2]} (and user:tom in the concrete)
	existing := *wst.wildcard
	updatedExclusions := make([]T, 0, len(existing.GetExcludedSubjects()))
	for _, existingExclusion := range existing.GetExcludedSubjects() {
		if existingExclusion.GetSubjectId() == concrete.GetSubjectId() {
			// If the conditional on the concrete is empty, then the concrete is always present, so
			// we remove the exclusion entirely.
			if concrete.GetCaveatExpression() == nil {
				continue
			}

			// Otherwise, the conditional expression for the new exclusion is the existing expression &&
			// the *inversion* of the concrete's expression, as the exclusion will only apply if the
			// concrete subject is not present and the exclusion's expression is true.
			exclusionConditionalExpression := caveatAnd(
				existingExclusion.GetCaveatExpression(),
				caveatInvert(concrete.GetCaveatExpression()),
			)

			updatedExclusions = append(updatedExclusions, wst.constructor(
				concrete.GetSubjectId(),
				exclusionConditionalExpression,
				nil,
				existingExclusion,
				concrete),
			)
		} else {
			updatedExclusions = append(updatedExclusions, existingExclusion)
		}
	}

	constructed := wst.constructor(
		tuple.PublicWildcard,
		existing.GetCaveatExpression(),
		updatedExclusions,
		existing)
	wst.wildcard = &constructed
}

func (wst *wildcardSubjectTracker[T]) subtractWildcard(foundWildcard T) []T {
	if wst.wildcard == nil {
		// Nothing to do if no wildcard present.
		return nil
	}

	// If there is no condition on the wildcard and the new wildcard has no exclusions, then this wildcard goes away.
	// Example: {*} - {*} => {}
	if foundWildcard.GetCaveatExpression() == nil && len(foundWildcard.GetExcludedSubjects()) == 0 {
		wst.wildcard = nil
		return nil
	}

	// Otherwise, we construct a new wildcard and return any concrete subjects that might result from this subtraction.
	existing := *wst.wildcard
	existingExclusions := wst.exclusionsMap()

	// Calculate the exclusions which turn into concrete subjects.
	// This occurs when a wildcard with exclusions is subtracted from a wildcard
	// (with, or without *matching* exclusions).
	//
	// Example:
	// Given the two wildcards `* - {user:sarah}` and `* - {user:tom, user:amy, user:sarah}`,
	// the resulting concrete subjects are {user:tom, user:amy} because the first set contains
	// `tom` and `amy` (but not `sarah`) and the second set contains all three.
	resultingConcreteSubjects := make([]T, 0, len(foundWildcard.GetExcludedSubjects()))
	for _, excludedSubject := range foundWildcard.GetExcludedSubjects() {
		if existingExclusion, isExistingExclusion := existingExclusions[excludedSubject.GetSubjectId()]; !isExistingExclusion || existingExclusion.GetCaveatExpression() != nil {
			// The conditional expression for the now-concrete subject type is the conditional on the provided exclusion
			// itself.
			//
			// As an example, subtracting the wildcards
			// {*[caveat1] - {user:tom}}
			// -
			// {*[caveat3] - {user:sarah[caveat4]}}
			//
			// the resulting expression to produce a *concrete* `user:sarah` is
			// `caveat1 && caveat3 && caveat4`, because the concrete subject only appears if the first
			// wildcard applies, the *second* wildcard applies and its exclusion applies.
			exclusionConditionalExpression := caveatAnd(
				caveatAnd(
					existing.GetCaveatExpression(),
					foundWildcard.GetCaveatExpression(),
				),
				excludedSubject.GetCaveatExpression(),
			)

			// If there is an existing exclusion, then its caveat expression is added as well, but inverted.
			//
			// As an example, subtracting the wildcards
			// {*[caveat1] - {user:tom[caveat2]}}
			// -
			// {*[caveat3] - {user:sarah[caveat4]}}
			//
			// the resulting expression to produce a *concrete* `user:sarah` is
			// `caveat1 && !caveat2 && caveat3 && caveat4`, because the concrete subject only appears
			// if the first wildcard applies, the *second* wildcard applies, the first exclusion
			// does *not* apply (ensuring the concrete is in the first wildcard) and the second exclusion
			// *does* apply (ensuring it is not in the second wildcard).
			if existingExclusion.GetCaveatExpression() != nil {
				exclusionConditionalExpression = caveatAnd(
					caveatAnd(
						caveatAnd(
							existing.GetCaveatExpression(),
							foundWildcard.GetCaveatExpression(),
						),
						caveatInvert(existingExclusion.GetCaveatExpression()),
					),
					excludedSubject.GetCaveatExpression(),
				)
			}

			resultingConcreteSubjects = append(resultingConcreteSubjects, wst.constructor(
				excludedSubject.GetSubjectId(),
				exclusionConditionalExpression,
				nil, excludedSubject))
		}
	}

	// Create the combined conditional: the wildcard can only exist when it is present and the other wildcard is not.
	combinedConditionalExpression := caveatAnd(existing.GetCaveatExpression(), caveatInvert(foundWildcard.GetCaveatExpression()))
	if combinedConditionalExpression != nil {
		constructed := wst.constructor(
			tuple.PublicWildcard,
			combinedConditionalExpression,
			existing.GetExcludedSubjects(),
			existing,
			foundWildcard)
		wst.wildcard = &constructed
	} else {
		// If there is no combined conditional expression, then the wildcard is completely removed from
		// the set.
		wst.wildcard = nil
	}

	return resultingConcreteSubjects
}

func (wst *wildcardSubjectTracker[T]) subtractConcrete(concrete T) {
	if wst.wildcard == nil {
		// Nothing to do if no wildcard present.
		return
	}

	// Subtracting a concrete type from a wildcard adds the concrete to the exclusions for the wildcard.
	// Examples:
	//  {*} - {user:tom} => {* - {user:tom}}
	//  {*} - {user:tom[c1]} => {* - {user:tom[c1]}}
	//  {* - {user:tom[c1]}} - {user:tom} => {* - {user:tom}}
	//  {* - {user:tom[c1]}} - {user:tom[c2]} => {* - {user:tom[c1 || c2]}}
	existing := *wst.wildcard
	updatedExclusions := make([]T, 0, len(existing.GetExcludedSubjects())+1)
	wasFound := false
	for _, existingExclusion := range existing.GetExcludedSubjects() {
		if existingExclusion.GetSubjectId() == concrete.GetSubjectId() {
			// The conditional expression for the exclusion is a combination on the existing exclusion or
			// the new expression. The caveat is short circuited here because if either the exclusion or
			// the concrete is non-caveated, then the whole exclusion is non-caveated.
			exclusionConditionalExpression := shortcircuitedOr(existingExclusion.GetCaveatExpression(), concrete.GetCaveatExpression())

			updatedExclusions = append(updatedExclusions, wst.constructor(
				concrete.GetSubjectId(),
				exclusionConditionalExpression,
				nil,
				existingExclusion,
				concrete),
			)
			wasFound = true
		} else {
			updatedExclusions = append(updatedExclusions, existingExclusion)
		}
	}

	if !wasFound {
		updatedExclusions = append(updatedExclusions, concrete)
	}

	constructed := wst.constructor(
		tuple.PublicWildcard,
		existing.GetCaveatExpression(),
		updatedExclusions,
		existing)
	wst.wildcard = &constructed
}

func (wst *wildcardSubjectTracker[T]) intersectDiffWithWildcard(other *wildcardSubjectTracker[T]) {
	if wst.wildcard == nil {
		// Nothing to do if no wildcard present.
		return
	}

	// If the other wildcard does not exist, change this wildcard to empty.
	if other.wildcard == nil {
		wst.wildcard = nil
		return
	}

	// If the other wildcard exists, then the intersection between the two wildcards is an && of
	// their conditionals, and a *union* of their exclusions.
	existing := *wst.wildcard
	foundWildcard := *other.wildcard

	concreteExclusions := newConcreteSubjectSet[T](wst.constructor)
	for _, excludedSubject := range existing.GetExcludedSubjects() {
		concreteExclusions.unionConcrete(excludedSubject)
	}

	for _, excludedSubject := range foundWildcard.GetExcludedSubjects() {
		concreteExclusions.unionConcrete(excludedSubject)
	}

	constructed := wst.constructor(
		tuple.PublicWildcard,
		caveatAnd(existing.GetCaveatExpression(), foundWildcard.GetCaveatExpression()),
		concreteExclusions.asSlice(),
		existing,
		foundWildcard)
	wst.wildcard = &constructed
}

// exclusionsMap returns a map from subject ID to the associated exclusion, for each exclusion
// on this wildcard.
func (wst *wildcardSubjectTracker[T]) exclusionsMap() map[string]T {
	if wst.wildcard == nil {
		return nil
	}

	return exclusionsMapFor(*wst.wildcard)
}

// exclusionsMapFor creates a map of all the exclusions on a wildcard, by subject ID.
func exclusionsMapFor[T Subject[T]](wildcard T) map[string]T {
	exclusions := make(map[string]T, len(wildcard.GetExcludedSubjects()))
	for _, excludedSubject := range wildcard.GetExcludedSubjects() {
		exclusions[excludedSubject.GetSubjectId()] = excludedSubject
	}
	return exclusions
}

// shortcircuitedOr combines two caveat expressions via an `||`. If one of the expressions is nil,
// then the entire expression is *short circuited*, and a nil is returned.
func shortcircuitedOr(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	if first == nil || second == nil {
		return nil
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_OR,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

// caveatOr `||`'s together two caveat expressions. If one expression is nil, the other is returned.
func caveatOr(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	if first == nil {
		return second
	}

	if second == nil {
		return first
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_OR,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

// caveatAnd `&&`'s together two caveat expressions. If one expression is nil, the other is returned.
func caveatAnd(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	if first == nil {
		return second
	}

	if second == nil {
		return first
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_AND,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

// caveatInvert returns the caveat expression with a `!` placed in front of it. If the expression is
// nil, returns nil.
func caveatInvert(ce *v1.CaveatExpression) *v1.CaveatExpression {
	if ce == nil {
		return nil
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_NOT,
				Children: []*v1.CaveatExpression{ce},
			},
		},
	}
}
