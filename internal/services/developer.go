package services

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/go-cmp/cmp"

	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/grpcutil"
	"github.com/authzed/spicedb/pkg/membership"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

type devServer struct {
	v0.UnimplementedDeveloperServiceServer
	grpcutil.IgnoreAuthMixin

	shareStore ShareStore
}

const maxDepth = 25

// NewDeveloperServer creates an instance of the developer server.
func NewDeveloperServer(store ShareStore) v0.DeveloperServiceServer {
	return &devServer{
		shareStore: store,
	}
}

func (ds *devServer) Share(ctx context.Context, req *v0.ShareRequest) (*v0.ShareResponse, error) {
	reference, err := ds.shareStore.StoreShared(SharedData{
		Version:          sharedDataVersion,
		NamespaceConfigs: req.NamespaceConfigs,
		RelationTuples:   req.RelationTuples,
		ValidationYaml:   req.ValidationYaml,
		AssertionsYaml:   req.AssertionsYaml,
	})
	if err != nil {
		return nil, err
	}

	return &v0.ShareResponse{
		ShareReference: reference,
	}, nil
}

func (ds *devServer) LookupShared(ctx context.Context, req *v0.LookupShareRequest) (*v0.LookupShareResponse, error) {
	shared, ok, err := ds.shareStore.LookupSharedByReference(req.ShareReference)
	if err != nil {
		return &v0.LookupShareResponse{
			Status: v0.LookupShareResponse_FAILED_TO_LOOKUP,
		}, nil
	}

	if !ok {
		return &v0.LookupShareResponse{
			Status: v0.LookupShareResponse_UNKNOWN_REFERENCE,
		}, nil
	}

	return &v0.LookupShareResponse{
		Status:           v0.LookupShareResponse_VALID_REFERENCE,
		NamespaceConfigs: shared.NamespaceConfigs,
		RelationTuples:   shared.RelationTuples,
		ValidationYaml:   shared.ValidationYaml,
		AssertionsYaml:   shared.AssertionsYaml,
	}, nil
}

func (ds *devServer) EditCheck(ctx context.Context, req *v0.EditCheckRequest) (*v0.EditCheckResponse, error) {
	devContext, ok, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &v0.EditCheckResponse{
			ContextNamespaces: devContext.Namespaces,
			AdditionalErrors:  devContext.Errors,
		}, nil
	}

	// Run the checks and store their output.
	var results []*v0.EditCheckResult
	for _, checkTpl := range req.CheckTuples {
		cr := devContext.Dispatcher.Check(ctx, graph.CheckRequest{
			Start:          checkTpl.ObjectAndRelation,
			Goal:           checkTpl.User.GetUserset(),
			AtRevision:     devContext.Revision,
			DepthRemaining: maxDepth,
		})
		if cr.Err != nil {
			validationErrs, wireErr := rewriteGraphError(v0.ValidationError_CHECK_WATCH, tuple.String(checkTpl), cr.Err)
			return &v0.EditCheckResponse{
				AdditionalErrors: validationErrs,
			}, wireErr
		}

		results = append(results, &v0.EditCheckResult{
			Tuple:    checkTpl,
			IsMember: cr.IsMember,
		})
	}

	return &v0.EditCheckResponse{
		ContextNamespaces: devContext.Namespaces,
		CheckResults:      results,
	}, nil
}

func (ds *devServer) Validate(ctx context.Context, req *v0.ValidateRequest) (*v0.ValidateResponse, error) {
	devContext, ok, err := NewDevContext(ctx, req.Context)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &v0.ValidateResponse{
			ContextNamespaces: devContext.Namespaces,
			ValidationErrors:  devContext.Errors,
		}, nil
	}

	// Parse the validation YAML.
	validation, err := validationfile.ParseValidationBlock([]byte(req.ValidationYaml))
	if err != nil {
		return &v0.ValidateResponse{
			ValidationErrors: []*v0.ValidationError{
				convertYamlError(v0.ValidationError_VALIDATION_YAML, err),
			},
		}, nil
	}

	// Parse the assertions YAML.
	assertions, err := validationfile.ParseAssertionsBlock([]byte(req.AssertionsYaml))
	if err != nil {
		return &v0.ValidateResponse{
			ValidationErrors: []*v0.ValidationError{
				convertYamlError(v0.ValidationError_ASSERTION, err),
			},
		}, nil
	}

	// Run assertions.
	assertTrueRelationships, aerr := assertions.AssertTrueRelationships()
	if aerr != nil {
		return &v0.ValidateResponse{
			ValidationErrors: []*v0.ValidationError{
				convertSourceError(v0.ValidationError_ASSERTION, aerr),
			},
		}, nil
	}

	assertFalseRelationships, aerr := assertions.AssertFalseRelationships()
	if aerr != nil {
		return &v0.ValidateResponse{
			ValidationErrors: []*v0.ValidationError{
				convertSourceError(v0.ValidationError_ASSERTION, aerr),
			},
		}, nil
	}

	trueFailures, err := runAssertions(ctx, devContext, assertTrueRelationships, true, "Expected relation or permission %s to exist")
	if err != nil {
		return nil, err
	}

	falseFailures, err := runAssertions(ctx, devContext, assertFalseRelationships, false, "Expected relation or permission %s to not exist")
	if err != nil {
		return nil, err
	}

	failures := append(trueFailures, falseFailures...)

	// Run validation.
	membershipSet, validationFailures, wireErr := runValidation(ctx, devContext, validation)
	if wireErr != nil {
		return nil, wireErr
	}

	// If requested, regenerate the validation YAML.
	updatedValidationYaml := ""
	if membershipSet != nil && req.UpdateValidationYaml {
		updatedValidationYaml, err = generateValidation(membershipSet)
		if err != nil {
			return nil, err
		}
	}

	return &v0.ValidateResponse{
		ContextNamespaces:     devContext.Namespaces,
		ValidationErrors:      append(failures, validationFailures...),
		UpdatedValidationYaml: updatedValidationYaml,
	}, nil
}

func runAssertions(ctx context.Context, devContext *DevContext, relationships []*v0.RelationTuple, expected bool, fmtString string) ([]*v0.ValidationError, error) {
	var failures []*v0.ValidationError
	for _, relationship := range relationships {
		cr := devContext.Dispatcher.Check(ctx, graph.CheckRequest{
			Start:          relationship.ObjectAndRelation,
			Goal:           relationship.User.GetUserset(),
			AtRevision:     devContext.Revision,
			DepthRemaining: maxDepth,
		})
		if cr.Err != nil {
			validationErrs, wireErr := rewriteGraphError(v0.ValidationError_ASSERTION, tuple.String(relationship), cr.Err)
			failures = append(failures, validationErrs...)
			if wireErr != nil {
				return nil, wireErr
			}
		} else if cr.IsMember != expected {
			failures = append(failures, &v0.ValidationError{
				Message:  fmt.Sprintf(fmtString, tuple.String(relationship)),
				Source:   v0.ValidationError_ASSERTION,
				Kind:     v0.ValidationError_ASSERTION_FAILED,
				Metadata: tuple.String(relationship),
			})
		}
	}

	return failures, nil
}

func generateValidation(membershipSet *membership.MembershipSet) (string, error) {
	validationMap := validationfile.ValidationMap{}
	subjectsByONR := membershipSet.SubjectsByONR()

	var onrStrings []string
	for onrString := range subjectsByONR {
		onrStrings = append(onrStrings, onrString)
	}

	// Sort to ensure stability of output.
	sort.Strings(onrStrings)

	for _, onrString := range onrStrings {
		foundSubjects := subjectsByONR[onrString]
		var strs []string
		for _, fs := range foundSubjects.ListFound() {
			strs = append(strs,
				fmt.Sprintf("[%s] is %s",
					tuple.StringONR(fs.Subject()),
					strings.Join(wrapRelationships(tuple.StringsONRs(fs.Relationships())), "/"),
				))
		}

		// Sort to ensure stability of output.
		sort.Strings(strs)

		var validationStrings []validationfile.ValidationString
		for _, s := range strs {
			validationStrings = append(validationStrings, validationfile.ValidationString(s))
		}

		validationMap[validationfile.ObjectRelationString(onrString)] = validationStrings
	}

	return validationMap.AsYAML()
}

func runValidation(ctx context.Context, devContext *DevContext, validation validationfile.ValidationMap) (*membership.MembershipSet, []*v0.ValidationError, error) {
	var failures []*v0.ValidationError
	membershipSet := membership.NewMembershipSet()

	for onrKey, validationStrings := range validation {
		// Unmarshal the ONR to expand from its string form.
		onr, err := onrKey.ONR()
		if err != nil {
			failures = append(failures,
				&v0.ValidationError{
					Message:  err.Error(),
					Source:   v0.ValidationError_VALIDATION_YAML,
					Kind:     v0.ValidationError_PARSE_ERROR,
					Metadata: err.Source,
				},
			)
			continue
		}

		// Run a full recursive expansion over the ONR.
		er := devContext.Dispatcher.Expand(ctx, graph.ExpandRequest{
			Start:          onr,
			AtRevision:     devContext.Revision,
			DepthRemaining: maxDepth,
			ExpansionMode:  graph.RecursiveExpansion,
		})
		if er.Err != nil {
			validationErrs, wireErr := rewriteGraphError(v0.ValidationError_VALIDATION_YAML, string(onrKey), er.Err)
			if validationErrs != nil {
				failures = append(failures, validationErrs...)
				continue
			}

			return nil, nil, wireErr
		}

		// Add the ONR and its expansion to the membership set.
		foundSubjects, _ := membershipSet.AddExpansion(onr, er.Tree)

		// Compare the terminal subjects found to those specified.
		errs := validateSubjects(onr, foundSubjects, validationStrings)
		failures = append(failures, errs...)
	}

	return membershipSet, failures, nil
}

func wrapRelationships(onrStrings []string) []string {
	var wrapped []string
	for _, str := range onrStrings {
		wrapped = append(wrapped, fmt.Sprintf("<%s>", str))
	}

	// Sort to ensure stability.
	sort.Strings(wrapped)
	return wrapped
}

func validateSubjects(onr *v0.ObjectAndRelation, fs membership.FoundSubjects, validationStrings []validationfile.ValidationString) []*v0.ValidationError {
	var failures []*v0.ValidationError

	// Verify that every referenced subject is found in the membership.
	encounteredSubjects := map[string]struct{}{}
	for _, validationString := range validationStrings {
		subjectONR, err := validationString.Subject()
		if err != nil {
			failures = append(failures, &v0.ValidationError{
				Message:  fmt.Sprintf("For object and permission/relation `%s`, %s", tuple.StringONR(onr), err.Error()),
				Source:   v0.ValidationError_VALIDATION_YAML,
				Kind:     v0.ValidationError_PARSE_ERROR,
				Metadata: fmt.Sprintf("[%s]", err.Source),
			})
			continue
		}

		if subjectONR == nil {
			continue
		}

		encounteredSubjects[tuple.StringONR(subjectONR)] = struct{}{}

		expectedRelationships, err := validationString.ONRS()
		if err != nil {
			failures = append(failures, &v0.ValidationError{
				Message:  fmt.Sprintf("For object and permission/relation `%s`, %s", tuple.StringONR(onr), err.Error()),
				Source:   v0.ValidationError_VALIDATION_YAML,
				Kind:     v0.ValidationError_PARSE_ERROR,
				Metadata: fmt.Sprintf("<%s>", err.Source),
			})
			continue
		}

		subject, ok := fs.LookupSubject(subjectONR)
		if !ok {
			failures = append(failures, &v0.ValidationError{
				Message:  fmt.Sprintf("For object and permission/relation `%s`, missing expected subject `%s`", tuple.StringONR(onr), tuple.StringONR(subjectONR)),
				Source:   v0.ValidationError_VALIDATION_YAML,
				Kind:     v0.ValidationError_MISSING_EXPECTED_TUPLE,
				Metadata: tuple.StringONR(subjectONR),
			})
			continue
		}

		foundRelationships := subject.Relationships()

		// Verify that the relationships are the same.
		expectedONRStrings := tuple.StringsONRs(expectedRelationships)
		foundONRStrings := tuple.StringsONRs(foundRelationships)
		if !cmp.Equal(expectedONRStrings, foundONRStrings) {
			failures = append(failures, &v0.ValidationError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, found different relationships for subject `%s`: Specified: `%s`, Computed: `%s`",
					tuple.StringONR(onr),
					tuple.StringONR(subjectONR),
					strings.Join(wrapRelationships(expectedONRStrings), "/"),
					strings.Join(wrapRelationships(foundONRStrings), "/"),
				),
				Source:   v0.ValidationError_VALIDATION_YAML,
				Kind:     v0.ValidationError_MISSING_EXPECTED_TUPLE,
				Metadata: string(validationString),
			})
		}
	}

	// Verify that every subject found was referenced.
	for _, foundSubject := range fs.ListFound() {
		_, ok := encounteredSubjects[tuple.StringONR(foundSubject.Subject())]
		if !ok {
			failures = append(failures, &v0.ValidationError{
				Message: fmt.Sprintf("For object and permission/relation `%s`, subject `%s` found but missing from specified",
					tuple.StringONR(onr),
					tuple.StringONR(foundSubject.Subject()),
				),
				Source:   v0.ValidationError_VALIDATION_YAML,
				Kind:     v0.ValidationError_EXTRA_TUPLE_FOUND,
				Metadata: tuple.StringONR(onr),
			})
		}
	}

	return failures
}

func rewriteGraphError(source v0.ValidationError_Source, metadata string, checkError error) ([]*v0.ValidationError, error) {
	var nsNotFoundError sharederrors.UnknownNamespaceError = nil
	var relNotFoundError sharederrors.UnknownRelationError = nil

	if errors.As(checkError, &nsNotFoundError) {
		return []*v0.ValidationError{
			&v0.ValidationError{
				Message:  checkError.Error(),
				Source:   source,
				Kind:     v0.ValidationError_UNKNOWN_NAMESPACE,
				Metadata: metadata,
			},
		}, nil
	}

	if errors.As(checkError, &relNotFoundError) {
		return []*v0.ValidationError{
			&v0.ValidationError{
				Message:  checkError.Error(),
				Source:   source,
				Kind:     v0.ValidationError_UNKNOWN_RELATION,
				Metadata: metadata,
			},
		}, nil
	}

	return nil, rewriteACLError(checkError)
}

func convertSourceError(source v0.ValidationError_Source, err *validationfile.ErrorWithSource) *v0.ValidationError {
	return &v0.ValidationError{
		Message:  err.Error(),
		Kind:     v0.ValidationError_PARSE_ERROR,
		Source:   source,
		Metadata: err.Source,
	}
}

var yamlLineRegex = regexp.MustCompile(`line ([0-9]+): (.+)`)

func convertYamlError(source v0.ValidationError_Source, err error) *v0.ValidationError {
	var lineNumber uint64 = 0
	var msg = err.Error()

	pieces := yamlLineRegex.FindStringSubmatch(err.Error())
	if len(pieces) == 3 {
		// We can safely ignore the error here because it will default to 0, which is the not found
		// case.
		lineNumber, _ = strconv.ParseUint(pieces[1], 10, 0)
		msg = pieces[2]
	}

	return &v0.ValidationError{
		Message: msg,
		Kind:    v0.ValidationError_PARSE_ERROR,
		Source:  source,
		Line:    uint32(lineNumber),
	}
}
