package util

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func caveat(name string) *core.ContextualizedCaveat {
	return &core.ContextualizedCaveat{
		CaveatName: name,
	}
}

func caveatexpr(name string) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Caveat{
			Caveat: caveat(name),
		},
	}
}

func sub(subjectID string) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId: subjectID,
	}
}

func csub(subjectID string, expr *v1.CaveatExpression) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        subjectID,
		CaveatExpression: expr,
	}
}

func cwc(expr *v1.CaveatExpression, exclusions ...*v1.FoundSubject) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        tuple.PublicWildcard,
		ExcludedSubjects: exclusions,
		CaveatExpression: expr,
	}
}

func wc(exclusions ...string) *v1.FoundSubject {
	excludedSubjects := make([]*v1.FoundSubject, 0, len(exclusions))
	for _, excludedID := range exclusions {
		excludedSubjects = append(excludedSubjects, &v1.FoundSubject{
			SubjectId: excludedID,
		})
	}

	return &v1.FoundSubject{
		SubjectId:        tuple.PublicWildcard,
		ExcludedSubjects: excludedSubjects,
	}
}

func TestSubjectSetAdd(t *testing.T) {
	tcs := []struct {
		name        string
		existing    []*v1.FoundSubject
		toAdd       *v1.FoundSubject
		expectedSet []*v1.FoundSubject
	}{
		{
			"basic add",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("baz"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), sub("baz")},
		},
		{
			"basic repeated add",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("bar"),
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
		},
		{
			"add of an empty wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			wc(),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			wc("1", "2"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
		},
		{
			"add of a wildcard to a wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
			wc(),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions to a bare wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
			wc("1", "2"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a bare wildcard to a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
			wc(),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions to a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
			wc("2", "3"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("2")},
		},
		{
			"add of a subject to a wildcard with exclusions that does not have that subject",
			[]*v1.FoundSubject{wc("1", "2")},
			sub("3"),
			[]*v1.FoundSubject{wc("1", "2"), sub("3")},
		},
		{
			"add of a subject to a wildcard with exclusions that has that subject",
			[]*v1.FoundSubject{wc("1", "2")},
			sub("2"),
			[]*v1.FoundSubject{wc("1"), sub("2")},
		},
		{
			"add of a subject to a wildcard with exclusions that has that subject",
			[]*v1.FoundSubject{sub("2")},
			wc("1", "2"),
			[]*v1.FoundSubject{wc("1"), sub("2")},
		},
		{
			"add of a subject to a bare wildcard",
			[]*v1.FoundSubject{wc()},
			sub("1"),
			[]*v1.FoundSubject{wc(), sub("1")},
		},
		{
			"add of two wildcards",
			[]*v1.FoundSubject{wc("1")},
			wc("2"),
			[]*v1.FoundSubject{wc()},
		},
		{
			"add of two wildcards with same restrictions",
			[]*v1.FoundSubject{wc("1")},
			wc("1", "2"),
			[]*v1.FoundSubject{wc("1")},
		},

		// Tests with caveats.
		{
			"basic add of caveated to non-caveated",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("testcaveat")),
				sub("bar"),
			},
			sub("foo"),
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
		},
		{
			"basic add of non-caveated to caveated",
			[]*v1.FoundSubject{
				sub("foo"),
				sub("bar"),
			},
			csub("foo", caveatexpr("testcaveat")),
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
		},
		{
			"basic add of caveated to caveated",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("testcaveat")),
				sub("bar"),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				sub("bar"),
				csub("foo", caveatOr(caveatexpr("testcaveat"), caveatexpr("anothercaveat"))),
			},
		},
		{
			"add of caveated wildcard to non-caveated",
			[]*v1.FoundSubject{
				cwc(caveatexpr("testcaveat")),
				sub("bar"),
			},
			wc(),
			[]*v1.FoundSubject{sub("bar"), wc()},
		},
		{
			"add of caveated wildcard to caveated",
			[]*v1.FoundSubject{
				cwc(caveatexpr("testcaveat")),
				sub("bar"),
			},
			cwc(caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				sub("bar"),
				cwc(caveatOr(caveatexpr("testcaveat"), caveatexpr("anothercaveat"))),
			},
		},
		{
			"add of wildcard to a caveated sub",
			[]*v1.FoundSubject{
				csub("bar", caveatexpr("testcaveat")),
			},
			wc(),
			[]*v1.FoundSubject{
				csub("bar", caveatexpr("testcaveat")),
				wc(),
			},
		},
		{
			"add caveated sub to wildcard with non-matching caveated exclusion",
			[]*v1.FoundSubject{
				cwc(nil, csub("bar", caveatexpr("testcaveat"))),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				cwc(nil, csub("bar", caveatexpr("testcaveat"))),
				csub("foo", caveatexpr("anothercaveat")),
			},
		},
		{
			"add caveated sub to wildcard with matching caveated exclusion",
			[]*v1.FoundSubject{
				cwc(nil, csub("foo", caveatexpr("testcaveat"))),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				// The caveat of the exclusion is now the combination of the caveats from the original
				// wildcard and the concrete which was added, as the exclusion applies when the exclusion
				// caveat is true and the concrete's caveat is false.
				cwc(nil, csub("foo",
					caveatAnd(
						caveatexpr("testcaveat"),
						caveatInvert(caveatexpr("anothercaveat")),
					),
				)),
				csub("foo", caveatexpr("anothercaveat")),
			},
		},
		{
			"add caveated sub to caveated wildcard with matching caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("testcaveat"))),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				// The caveat of the exclusion is now the combination of the caveats from the original
				// wildcard and the concrete which was added, as the exclusion applies when the exclusion
				// caveat is true and the concrete's caveat is false.
				cwc(caveatexpr("wildcardcaveat"), csub("foo",
					caveatAnd(
						caveatexpr("testcaveat"),
						caveatInvert(caveatexpr("anothercaveat")),
					),
				)),
				csub("foo", caveatexpr("anothercaveat")),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.Add(existing)
			}

			existingSet.Add(tc.toAdd)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()

			sort.Sort(sortByID(expectedSet))
			sort.Sort(sortByID(computedSet))

			stableSortExclusions(expectedSet)
			stableSortExclusions(computedSet)

			require.Equal(t, len(expectedSet), len(computedSet), "got: %v", computedSet)
			for index := range expectedSet {
				require.True(t, proto.Equal(expectedSet[index], computedSet[index]), "got mismatch: %v vs %v", expectedSet[index], computedSet[index])
			}
		})
	}
}

func TestSubjectSetSubtract(t *testing.T) {
	tcs := []struct {
		name        string
		existing    []*v1.FoundSubject
		toSubtract  *v1.FoundSubject
		expectedSet []*v1.FoundSubject
	}{
		{
			"basic subtract, no overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("baz"),
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
		},
		{
			"basic subtract, with overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("bar"),
			[]*v1.FoundSubject{sub("foo")},
		},
		{
			"basic subtract from bare wildcard",
			[]*v1.FoundSubject{sub("foo"), wc()},
			sub("bar"),
			[]*v1.FoundSubject{sub("foo"), wc("bar")},
		},
		{
			"subtract from bare wildcard and set",
			[]*v1.FoundSubject{sub("bar"), wc()},
			sub("bar"),
			[]*v1.FoundSubject{wc("bar")},
		},
		{
			"subtract from wildcard",
			[]*v1.FoundSubject{sub("bar"), wc("bar")},
			sub("bar"),
			[]*v1.FoundSubject{wc("bar")},
		},
		{
			"subtract from wildcard with existing exclusions",
			[]*v1.FoundSubject{sub("bar"), wc("hiya")},
			sub("bar"),
			[]*v1.FoundSubject{wc("hiya", "bar")},
		},
		{
			"subtract bare wildcard from set",
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract wildcard with no matching exclusions from set",
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
			wc("baz"),
			[]*v1.FoundSubject{},
		},
		{
			"subtract wildcard with matching exclusions from set",
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
			wc("bar"),
			[]*v1.FoundSubject{sub("bar")},
		},
		{
			"subtract wildcard from another wildcard, both with the same exclusions",
			[]*v1.FoundSubject{wc("sarah")},
			wc("sarah"),
			[]*v1.FoundSubject{},
		},
		{
			"subtract wildcard from another wildcard, with different exclusions",
			[]*v1.FoundSubject{wc("tom"), sub("foo"), sub("bar")},
			wc("sarah"),
			[]*v1.FoundSubject{sub("sarah")},
		},
		{
			"subtract wildcard from another wildcard, with more exclusions",
			[]*v1.FoundSubject{wc("sarah")},
			wc("sarah", "tom"),
			[]*v1.FoundSubject{sub("tom")},
		},

		// Tests with caveats.
		{
			"subtract non-caveated from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("somecaveat"))},
			sub("foo"),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated from non-caveated",
			[]*v1.FoundSubject{sub("foo")},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear now when the caveat is false.
				csub("foo", caveatInvert(caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract caveated from caveated",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("startingcaveat")),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear when its caveat is true, and the subtracted caveat
				// is false.
				csub("foo", caveatAnd(caveatexpr("startingcaveat"), caveatInvert(caveatexpr("somecaveat")))),
			},
		},
		{
			"subtract caveated bare wildcard from non-caveated",
			[]*v1.FoundSubject{sub("foo")},
			cwc(caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear now when the caveat is false.
				csub("foo", caveatInvert(caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract bare wildcard from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("something"))},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated bare wildcard from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("something"))},
			cwc(caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear now when its caveat is true and the wildcard's caveat
				// is false.
				csub("foo", caveatAnd(caveatexpr("something"), caveatInvert(caveatexpr("somecaveat")))),
			},
		},
		{
			"subtract wildcard with caveated exception from non-caveated",
			[]*v1.FoundSubject{sub("foo")},
			cwc(nil, csub("foo", caveatexpr("somecaveat"))),
			[]*v1.FoundSubject{
				// The subject should only appear now when somecaveat is true, making the exclusion present
				// on the wildcard, and thus preventing the wildcard from removing the concrete subject.
				csub("foo", caveatexpr("somecaveat")),
			},
		},
		{
			"subtract wildcard with caveated exception from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("somecaveat"))},
			cwc(nil, csub("foo", caveatexpr("anothercaveat"))),
			[]*v1.FoundSubject{
				// The subject should only appear now when both caveats are true.
				csub("foo", caveatAnd(caveatexpr("somecaveat"), caveatexpr("anothercaveat"))),
			},
		},
		{
			"subtract caveated wildcard with caveated exception from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("somecaveat"))},
			cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("anothercaveat"))),
			[]*v1.FoundSubject{
				// The subject should appear when:
				// somecaveat && (!wildcardcaveat || anothercaveat)
				csub("foo", caveatAnd(caveatexpr("somecaveat"), caveatOr(caveatInvert(caveatexpr("wildcardcaveat")), caveatexpr("anothercaveat")))),
			},
		},
		{
			"subtract caveated concrete from non-caveated wildcard",
			[]*v1.FoundSubject{wc()},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should be a caveated exception on the wildcard.
				cwc(nil, csub("foo", caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract caveated concrete from caveated wildcard",
			[]*v1.FoundSubject{cwc(caveatexpr("wildcardcaveat"))},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should be a caveated exception on the wildcard.
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract caveated concrete from wildcard with exclusion",
			[]*v1.FoundSubject{
				wc("foo"),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// No change, as `foo` is always removed.
				wc("foo"),
			},
		},
		{
			"subtract caveated concrete from caveated wildcard with exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), sub("foo")),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// No change, as `foo` is always removed.
				cwc(caveatexpr("wildcardcaveat"), sub("foo")),
			},
		},
		{
			"subtract caveated concrete from caveated wildcard with caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("exclusioncaveat"))),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should be excluded now if *either* caveat is true.
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatOr(caveatexpr("exclusioncaveat"), caveatexpr("somecaveat")))),
			},
		},
		{
			"subtract non-caveated bare wildcard from caveated wildcard",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat")),
			},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated bare wildcard from non-caveated wildcard",
			[]*v1.FoundSubject{
				wc(),
			},
			cwc(caveatexpr("wildcardcaveat")),
			[]*v1.FoundSubject{
				// The new wildcard is caveated on the inversion of the caveat.
				cwc(caveatInvert(caveatexpr("wildcardcaveat"))),
			},
		},
		{
			"subtract non-caveated bare wildcard from caveated wildcard with exclusions",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), sub("foo")),
			},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated bare wildcard from non-caveated wildcard with exclusions",
			[]*v1.FoundSubject{
				wc("foo"),
			},
			cwc(caveatexpr("wildcardcaveat")),
			[]*v1.FoundSubject{
				// The new wildcard is caveated on the inversion of the caveat.
				cwc(caveatInvert(caveatexpr("wildcardcaveat")), sub("foo")),
			},
		},
		{
			"subtract caveated wildcard with caveated exclusion from caveated wildcard with caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat1"), csub("foo", caveatexpr("exclusion1"))),
			},
			cwc(caveatexpr("wildcardcaveat2"), csub("foo", caveatexpr("exclusion2"))),
			[]*v1.FoundSubject{
				cwc(
					// The wildcard itself only appears if caveat1 is true and caveat2 is *false*.
					caveatAnd(caveatexpr("wildcardcaveat1"), caveatInvert(caveatexpr("wildcardcaveat2"))),

					// The exclusion still only applies if exclusion1 is true, because if it is false,
					// it doesn't matter that the subject was excluded from the subtraction.
					csub("foo", caveatexpr("exclusion1")),
				),

				// A concrete of foo is produced when all of these are true:
				// 1) the first wildcard is present
				// 2) the second wildcard is present
				// 3) the exclusion on the first wildcard is false, meaning the concrete is present within
				//    the wildcard
				// 4) the exclusion within the second is true, meaning that the concrete was not present
				//
				// thus causing the expression to become `{*} - {* - {foo}}`, and therefore producing
				// `foo` as a concrete.
				csub("foo",
					caveatAnd(
						caveatAnd(
							caveatAnd(
								caveatexpr("wildcardcaveat1"),
								caveatexpr("wildcardcaveat2"),
							),
							caveatInvert(caveatexpr("exclusion1")),
						),
						caveatexpr("exclusion2"),
					),
				),
			},
		},
		{
			"subtract caveated wildcard with caveated exclusion from caveated wildcard with different caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat1"), csub("foo", caveatexpr("exclusion1"))),
			},
			cwc(caveatexpr("wildcardcaveat2"), csub("bar", caveatexpr("exclusion2"))),
			[]*v1.FoundSubject{
				cwc(
					// The wildcard itself only appears if caveat1 is true and caveat2 is *false*.
					caveatAnd(caveatexpr("wildcardcaveat1"), caveatInvert(caveatexpr("wildcardcaveat2"))),

					// The foo exclusion remains the same.
					csub("foo", caveatexpr("exclusion1")),
				),

				// Because `bar` is excluded in the *subtraction*, it can appear as a *concrete* subject
				// in the scenario where the first wildcard is applied, the second wildcard is applied
				// and its own exclusion is true.
				// Therefore, bar must be *concretely* in the set if:
				// wildcard1 && wildcard2 && exclusion2
				csub("bar",
					caveatAnd(
						caveatAnd(
							caveatexpr("wildcardcaveat1"),
							caveatexpr("wildcardcaveat2"),
						),
						caveatexpr("exclusion2"),
					),
				),
			},
		},
		{
			"concretes with wildcard with partially matching exclusions subtracted",
			[]*v1.FoundSubject{
				sub("foo"),
				sub("bar"),
			},
			cwc(caveatexpr("caveat"), sub("foo")),
			[]*v1.FoundSubject{
				sub("foo"),
				csub("bar", caveatInvert(caveatexpr("caveat"))),
			},
		},
		{
			"subtract caveated from caveated (same caveat)",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("somecaveat")),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// Since no expression simplification is occurring, subtracting a caveated concrete
				// subject from another with the *same* caveat, results in an expression that &&'s
				// together the expression and its inversion. In practice, this will simplify down to
				// nothing, but that happens at a different layer.
				csub("foo", caveatAnd(caveatexpr("somecaveat"), caveatInvert(caveatexpr("somecaveat")))),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.Add(existing)
			}

			existingSet.Subtract(tc.toSubtract)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()

			sort.Sort(sortByID(expectedSet))
			sort.Sort(sortByID(computedSet))

			stableSortExclusions(expectedSet)
			stableSortExclusions(computedSet)

			require.Equal(t, len(expectedSet), len(computedSet), "got: %v", computedSet)
			for index := range expectedSet {
				require.True(t, proto.Equal(expectedSet[index], computedSet[index]), "got mismatch: expected `%v`, got `%v`", expectedSet[index], computedSet[index])
			}
		})
	}
}

func TestSubjectSetIntersection(t *testing.T) {
	tcs := []struct {
		name                string
		existing            []*v1.FoundSubject
		toIntersect         []*v1.FoundSubject
		expectedSet         []*v1.FoundSubject
		expectedInvertedSet []*v1.FoundSubject
	}{
		{
			"basic intersection, full overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			nil,
		},
		{
			"basic intersection, partial overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{sub("foo")},
			nil,
		},
		{
			"basic intersection, no overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("baz")},
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection between bare wildcard and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{sub("foo")},
			nil,
		},
		{
			"intersection between wildcard with exclusions and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc("tom")},
			[]*v1.FoundSubject{sub("foo")},
			nil,
		},
		{
			"intersection between wildcard with matching exclusions and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc("foo")},
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection between bare wildcards",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc()},
			nil,
		},
		{
			"intersection between bare wildcard and one with exclusions",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc("1", "2")},
			[]*v1.FoundSubject{wc("1", "2")},
			nil,
		},
		{
			"intersection between wildcards",
			[]*v1.FoundSubject{wc("2", "3")},
			[]*v1.FoundSubject{wc("1", "2")},
			[]*v1.FoundSubject{wc("1", "2", "3")},
			nil,
		},
		{
			"intersection wildcard with exclusions and concrete",
			[]*v1.FoundSubject{wc("2", "3")},
			[]*v1.FoundSubject{sub("4")},
			[]*v1.FoundSubject{sub("4")},
			nil,
		},
		{
			"intersection wildcard with matching exclusions and concrete",
			[]*v1.FoundSubject{wc("2", "3", "4")},
			[]*v1.FoundSubject{sub("4")},
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection of wildcards and two concrete types",
			[]*v1.FoundSubject{wc(), sub("1")},
			[]*v1.FoundSubject{wc(), sub("2")},
			[]*v1.FoundSubject{wc(), sub("1"), sub("2")},
			nil,
		},

		// Tests with caveats.
		{
			"intersection of non-caveated concrete and caveated concrete",
			[]*v1.FoundSubject{sub("1")},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},

			// Resulting subject is caveated on the caveat.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},
			nil,
		},
		{
			"intersection of caveated concrete and non-caveated concrete",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},
			[]*v1.FoundSubject{sub("1")},

			// Resulting subject is caveated on the caveat.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},
			nil,
		},
		{
			"intersection of caveated concrete and caveated concrete",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat2"))},

			// Resulting subject is caveated both caveats.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("caveat2")))},

			// Inverted result has the caveats reversed in the `&&` expression.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat2"), caveatexpr("caveat1")))},
		},
		{
			"intersection of caveated concrete and non-caveated bare wildcard",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			[]*v1.FoundSubject{wc()},

			// Resulting subject is caveated and concrete.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			nil,
		},
		{
			"intersection of non-caveated bare wildcard and caveated concrete",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Resulting subject is caveated and concrete.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			nil,
		},
		{
			"intersection of caveated concrete and caveated bare wildcard",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			[]*v1.FoundSubject{cwc(caveatexpr("caveat2"))},

			// Resulting subject is caveated from both and concrete.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("caveat2")))},
			nil,
		},
		{
			"intersection of caveated bare wildcard and caveated concrete",
			[]*v1.FoundSubject{cwc(caveatexpr("caveat2"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Resulting subject is caveated from both and concrete.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("caveat2")))},
			nil,
		},
		{
			"intersection of caveated concrete and non-caveated wildcard with non-matching exclusion",
			[]*v1.FoundSubject{wc("2")},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Resulting subject is caveated and concrete.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			nil,
		},
		{
			"intersection of caveated concrete and non-caveated wildcard with matching exclusion",
			[]*v1.FoundSubject{wc("1")},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Empty since the subject was in the exclusion set.
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection of caveated concrete and caveated wildcard with non-matching exclusion",
			[]*v1.FoundSubject{cwc(caveatexpr("wcaveat"), sub("2"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Since the wildcard is caveated and has a non-matching exclusion, the caveat is added to
			// the subject.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("wcaveat")))},
			nil,
		},
		{
			"intersection of caveated concrete and caveated wildcard with matching exclusion",
			[]*v1.FoundSubject{cwc(caveatexpr("wcaveat"), sub("1"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Since the wildcard is caveated but has a matching exclusion, the result is empty.
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection of caveated concrete and caveated wildcard with matching caveated exclusion",
			[]*v1.FoundSubject{cwc(caveatexpr("wcaveat"), csub("1", caveatexpr("ecaveat")))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			[]*v1.FoundSubject{
				// The concrete is included if its own caveat is true, the wildcard's caveat is true
				// and the exclusion's caveat is false.
				csub("1",
					caveatAnd(
						caveatexpr("caveat1"),
						caveatAnd(
							caveatexpr("wcaveat"),
							caveatInvert(caveatexpr("ecaveat")),
						),
					),
				),
			},
			nil,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.Add(existing)
			}

			toIntersect := NewSubjectSet()
			for _, toAdd := range tc.toIntersect {
				toIntersect.Add(toAdd)
			}

			existingSet.IntersectionDifference(toIntersect)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()

			sort.Sort(sortByID(expectedSet))
			sort.Sort(sortByID(computedSet))

			stableSortExclusions(expectedSet)
			stableSortExclusions(computedSet)

			require.Equal(t, len(expectedSet), len(computedSet), "got: %v", computedSet)
			for index := range expectedSet {
				require.True(t, proto.Equal(expectedSet[index], computedSet[index]), "got mismatch: expected %v vs found %v", expectedSet[index], computedSet[index])
			}

			// Run the intersection inverted, which should always result in the same results.
			t.Run("inverted", func(t *testing.T) {
				existingSet := NewSubjectSet()
				for _, existing := range tc.existing {
					existingSet.Add(existing)
				}

				toIntersect := NewSubjectSet()
				for _, toAdd := range tc.toIntersect {
					toIntersect.Add(toAdd)
				}

				toIntersect.IntersectionDifference(existingSet)

				// The inverted set is necessary for some caveated sets, because their expressions may
				// have references in reversed locations.
				expectedSet := tc.expectedSet
				if tc.expectedInvertedSet != nil {
					expectedSet = tc.expectedInvertedSet
				}

				computedSet := toIntersect.AsSlice()

				sort.Sort(sortByID(expectedSet))
				sort.Sort(sortByID(computedSet))

				stableSortExclusions(expectedSet)
				stableSortExclusions(computedSet)

				require.Equal(t, len(expectedSet), len(computedSet), "got: %v", computedSet)
				for index := range expectedSet {
					require.True(t, proto.Equal(expectedSet[index], computedSet[index]), "got inverted mismatch: expected %v vs found %v", expectedSet[index], computedSet[index])
				}
			})
		})
	}
}

var testSets = [][]*v1.FoundSubject{
	{sub("foo"), sub("bar")},
	{sub("foo")},
	{sub("baz")},
	{wc()},
	{wc("tom")},
	{wc("1", "2")},
	{wc("1", "2", "3")},
	{wc("2", "3")},
	{sub("1")},
	{wc(), sub("1"), sub("2")},
	{wc(), sub("2")},
	{wc(), sub("1")},
	{csub("1", caveatexpr("caveat"))},
	{csub("1", caveatexpr("caveat2"))},
	{cwc(caveatexpr("caveat2"))},
	{cwc(caveatexpr("wcaveat"), sub("1"))},
}

func TestUnionCommutativity(t *testing.T) {
	for _, pair := range allSubsets(testSets, 2) {
		t.Run(fmt.Sprintf("%v", pair), func(t *testing.T) {
			left1, left2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range pair[0] {
				left1.Add(l)
				left2.Add(l)
			}
			right1, right2 := NewSubjectSet(), NewSubjectSet()
			for _, r := range pair[1] {
				right1.Add(r)
				right2.Add(r)
			}
			// left union right
			left1.UnionWithSet(right1)
			// right union left
			right2.UnionWithSet(left2)

			mergedLeft := left1.AsSlice()
			mergedRight := right2.AsSlice()

			sort.Sort(sortByID(mergedLeft))
			sort.Sort(sortByID(mergedRight))
			stableSortExclusions(mergedLeft)
			stableSortExclusions(mergedRight)
			stableSortOrCaveats(mergedLeft)
			stableSortOrCaveats(mergedRight)

			require.Equal(t, len(mergedLeft), len(mergedRight))
			for index := range mergedLeft {
				require.True(t, proto.Equal(mergedLeft[index], mergedRight[index]), "pair: %v\nexpected %v\nfound %v", pair, mergedLeft[index], mergedRight[index])
			}
		})
	}
}

func TestUnionAssociativity(t *testing.T) {
	for _, triple := range allSubsets(testSets, 3) {
		t.Run(fmt.Sprintf("%v", triple), func(t *testing.T) {

			// A U (B U C) == (A U B) U C

			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				A1.Add(l)
				A2.Add(l)
			}
			B1, B2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				B1.Add(l)
				B2.Add(l)
			}
			C1, C2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				C1.Add(l)
				C2.Add(l)
			}

			// A U (B U C)
			B1.UnionWithSet(C1)
			A1.UnionWithSet(B1)

			// (A U B) U C
			A2.UnionWithSet(B2)
			A2.UnionWithSet(C2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()

			sort.Sort(sortByID(mergedLeft))
			sort.Sort(sortByID(mergedRight))
			stableSortExclusions(mergedLeft)
			stableSortExclusions(mergedRight)
			stableSortOrCaveats(mergedLeft)
			stableSortOrCaveats(mergedRight)

			require.Equal(t, len(mergedLeft), len(mergedRight))
			for index := range mergedLeft {
				require.True(t, proto.Equal(mergedLeft[index], mergedRight[index]), "triple: %v\nexpected %v\nfound %v", triple, mergedLeft[index], mergedRight[index])
			}
		})
	}
}

func TestIntersectionCommutativity(t *testing.T) {
	for _, pair := range allSubsets(testSets, 2) {
		t.Run(fmt.Sprintf("%v", pair), func(t *testing.T) {
			left1, left2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range pair[0] {
				left1.Add(l)
				left2.Add(l)
			}
			right1, right2 := NewSubjectSet(), NewSubjectSet()
			for _, r := range pair[1] {
				right1.Add(r)
				right2.Add(r)
			}
			// left intersect right
			left1.IntersectionDifference(right1)
			// right intersects left
			right2.IntersectionDifference(left2)

			mergedLeft := left1.AsSlice()
			mergedRight := right2.AsSlice()

			sort.Sort(sortByID(mergedLeft))
			sort.Sort(sortByID(mergedRight))
			stableSortExclusions(mergedLeft)
			stableSortExclusions(mergedRight)
			stableSortOrCaveats(mergedLeft)
			stableSortOrCaveats(mergedRight)
			stableSortAndCaveats(mergedLeft)
			stableSortAndCaveats(mergedRight)

			require.Equal(t, len(mergedLeft), len(mergedRight))
			for index := range mergedLeft {
				require.True(t, proto.Equal(mergedLeft[index], mergedRight[index]), "pair: %v\nexpected %v\nfound %v", pair, mergedLeft[index], mergedRight[index])
			}
		})
	}
}

func TestIntersectionAssociativity(t *testing.T) {
	for _, triple := range allSubsets(testSets, 3) {
		t.Run(fmt.Sprintf("%v", triple), func(t *testing.T) {

			// A ∩ (B ∩ C) == (A ∩ B) ∩ C

			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				A1.Add(l)
				A2.Add(l)
			}
			B1, B2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				B1.Add(l)
				B2.Add(l)
			}
			C1, C2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				C1.Add(l)
				C2.Add(l)
			}

			// A ∩ (B ∩ C)
			B1.IntersectionDifference(C1)
			A1.IntersectionDifference(B1)

			// (A ∩ B) ∩ C
			A2.IntersectionDifference(B2)
			A2.IntersectionDifference(C2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()

			sort.Sort(sortByID(mergedLeft))
			sort.Sort(sortByID(mergedRight))
			stableSortExclusions(mergedLeft)
			stableSortExclusions(mergedRight)
			stableSortOrCaveats(mergedLeft)
			stableSortOrCaveats(mergedRight)

			require.Equal(t, len(mergedLeft), len(mergedRight))
			for index := range mergedLeft {
				require.True(t, proto.Equal(mergedLeft[index], mergedRight[index]), "triple: %v\nexpected %v\nfound %v", triple, mergedLeft[index], mergedRight[index])
			}
		})
	}
}

func TestIdempotentUnion(t *testing.T) {
	for _, set := range testSets {
		t.Run(fmt.Sprintf("%v", set), func(t *testing.T) {
			// A U A == A
			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range set {
				A1.Add(l)
				A2.Add(l)
			}
			A1.UnionWithSet(A2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()

			sort.Sort(sortByID(mergedLeft))
			sort.Sort(sortByID(mergedRight))
			stableSortExclusions(mergedLeft)
			stableSortExclusions(mergedRight)
			stableSortOrCaveats(mergedLeft)
			stableSortOrCaveats(mergedRight)

			require.Equal(t, len(mergedLeft), len(mergedRight))
			for index := range mergedLeft {
				require.True(t, proto.Equal(mergedLeft[index], mergedRight[index]), "set: %v\nexpected %v\nfound %v", set, mergedLeft[index], mergedRight[index])
			}
		})
	}
}

func TestIdempotentIntersection(t *testing.T) {
	for _, set := range testSets {
		t.Run(fmt.Sprintf("%v", set), func(t *testing.T) {
			// A ∩ A == A
			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range set {
				A1.Add(l)
				A2.Add(l)
			}
			A1.IntersectionDifference(A2)
			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()

			sort.Sort(sortByID(mergedLeft))
			sort.Sort(sortByID(mergedRight))
			stableSortExclusions(mergedLeft)
			stableSortExclusions(mergedRight)
			stableSortOrCaveats(mergedLeft)
			stableSortOrCaveats(mergedRight)

			require.Equal(t, len(mergedLeft), len(mergedRight))
			for index := range mergedLeft {
				require.True(t, proto.Equal(mergedLeft[index], mergedRight[index]), "set: %v\nexpected %v\nfound %v", set, mergedLeft[index], mergedRight[index])
			}
		})
	}
}

// allSubsets returns a list of all subsets of length n
// it counts in binary and "activates" input funcs that match 1s in the binary representation
// it doesn't check for overflow so don't go crazy
func allSubsets[T any](objs []T, n int) [][]T {
	maxInt := uint(math.Exp2(float64(len(objs)))) - 1
	all := make([][]T, 0)

	for i := uint(0); i < maxInt; i++ {
		set := make([]T, 0, n)
		for digit := uint(0); digit < uint(len(objs)); digit++ {
			mask := uint(1) << digit
			if mask&i != 0 {
				set = append(set, objs[digit])
			}
			if len(set) > n {
				break
			}
		}
		if len(set) == n {
			all = append(all, set)
		}
	}
	return all
}

type sortByID []*v1.FoundSubject

func (a sortByID) Len() int           { return len(a) }
func (a sortByID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortByID) Less(i, j int) bool { return strings.Compare(a[i].SubjectId, a[j].SubjectId) < 0 }

func stableSortExclusions(fss []*v1.FoundSubject) {
	for _, fs := range fss {
		sort.Sort(sortByID(fs.ExcludedSubjects))
	}
}

func stableSortOrCaveats(fss []*v1.FoundSubject) {
	for _, fs := range fss {
		if fs.CaveatExpression != nil && fs.CaveatExpression.GetOperation() != nil && fs.CaveatExpression.GetOperation().Op == v1.CaveatOperation_OR {
			childCaveats := []string{}
			op := fs.CaveatExpression.GetOperation()
			for _, child := range op.GetChildren() {
				if child.GetCaveat() != nil {
					childCaveats = append(childCaveats, child.GetCaveat().CaveatName)
				}
			}

			sort.Strings(childCaveats)

			if len(childCaveats) == len(fs.CaveatExpression.GetOperation().GetChildren()) {
				updatedChildren := make([]*v1.CaveatExpression, 0, len(childCaveats))
				for _, caveatName := range childCaveats {
					updatedChildren = append(updatedChildren, caveatexpr(caveatName))
				}
				op.Children = updatedChildren
			}
		}
	}
}

func stableSortAndCaveats(fss []*v1.FoundSubject) {
	for _, fs := range fss {
		if fs.CaveatExpression != nil && fs.CaveatExpression.GetOperation() != nil && fs.CaveatExpression.GetOperation().Op == v1.CaveatOperation_AND {
			childCaveats := []string{}
			op := fs.CaveatExpression.GetOperation()
			for _, child := range op.GetChildren() {
				if child.GetCaveat() != nil {
					childCaveats = append(childCaveats, child.GetCaveat().CaveatName)
				}
			}

			sort.Strings(childCaveats)

			if len(childCaveats) == len(fs.CaveatExpression.GetOperation().GetChildren()) {
				updatedChildren := make([]*v1.CaveatExpression, 0, len(childCaveats))
				for _, caveatName := range childCaveats {
					updatedChildren = append(updatedChildren, caveatexpr(caveatName))
				}
				op.Children = updatedChildren
			}
		}
	}
}
