/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reconciler

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
)

// ==========================================================================
// Harness: converts minimal test input into the objects
// validateClaimAccessModesAgainstClass needs, runs it, and asserts on the
// result.
// ==========================================================================

// allAccessModes is every possible BucketAccessMode value, used to build "disallow everything"
// cases.
var allAccessModes = []cosiapi.BucketAccessMode{
	cosiapi.BucketAccessModeReadWrite,
	cosiapi.BucketAccessModeReadOnly,
	cosiapi.BucketAccessModeWriteOnly,
}

// accessModeCase is minimal input for one validateClaimAccessModesAgainstClass case: which
// access modes are requested per category, which modes the class disallows per category, and
// which violations are expected to fire.
type accessModeCase struct {
	name string

	// requested modes per category; the zero value means the category was not requested.
	objectData, objectMetadata, bucketMetadata cosiapi.BucketAccessMode

	// modes disallowed by the BucketAccessClass, per category.
	disallowObjectData, disallowObjectMetadata, disallowBucketMetadata []cosiapi.BucketAccessMode

	// wantViolations names every rule expected to fire: "noModeSet", "objectData",
	// "objectMetadata", "bucketMetadata". Nil/empty means no errors are expected.
	wantViolations []string
}

func (tc accessModeCase) run(t *testing.T) {
	t.Helper()

	claimRef := cosiapi.BucketClaimAccess{
		BucketClaimName: "claim",
		AccessModes: cosiapi.BucketAccessModes{
			ObjectData:     tc.objectData,
			ObjectMetadata: tc.objectMetadata,
			BucketMetadata: tc.bucketMetadata,
		},
	}
	disallowed := cosiapi.DisallowedBucketAccessModes{
		ObjectData:     tc.disallowObjectData,
		ObjectMetadata: tc.disallowObjectMetadata,
		BucketMetadata: tc.disallowBucketMetadata,
	}

	errs := validateClaimAccessModesAgainstClass(disallowed, claimRef)
	assert.ElementsMatch(t, tc.wantViolations, violationTags(t, errs))
}

// violationTags classifies each error returned by validateClaimAccessModesAgainstClass into the
// rule name that produced it, failing the test if an error doesn't match any known rule. Keeping
// this in sync with validateClaimAccessModesAgainstClass's error messages is intentional: adding a
// new rule there without teaching this function about it will fail every case that hits it.
func violationTags(t *testing.T, errs []error) []string {
	t.Helper()

	tags := make([]string, 0, len(errs))
	for _, err := range errs {
		msg := err.Error()
		switch {
		case strings.Contains(msg, "must set at least one of"):
			tags = append(tags, "noModeSet")
		case strings.Contains(msg, "objectData access mode"):
			tags = append(tags, "objectData")
		case strings.Contains(msg, "objectMetadata access mode"):
			tags = append(tags, "objectMetadata")
		case strings.Contains(msg, "bucketMetadata access mode"):
			tags = append(tags, "bucketMetadata")
		default:
			t.Fatalf("unrecognized violation error: %v", err)
		}
	}
	return tags
}

// ==========================================================================
// Tests: input and expected validation only.
// ==========================================================================

// Cases are ordered easiest to hardest, and grouped to make the coverage story readable:
//
//  1. The simplest possible input: nothing requested at all.
//  2. One category at a time, each paired as allowed-then-disallowed, cycling through a
//     different BucketAccessMode value per category so all three modes (ReadWrite, ReadOnly,
//     WriteOnly) are each exercised at least once.
//  3. All three categories requested together, nothing disallowed.
//  4. All three categories disallowed entirely (every mode blocked), but only one requested.
//  5. The combination of 3 and 4: everything requested, and everything disallowed.
//  6. Combined coverage cases: a single BucketAccessClass disallows a different subset of modes per
//     category (not everything, unlike case 4/5), checked against a fully-allowed request, a
//     fully-disallowed request, and a request that mixes both.
func Test_validateClaimAccessModesAgainstClass(t *testing.T) {
	tests := []accessModeCase{
		// 1. Simplest input: no category requested at all.
		{
			name:           "nothing requested",
			wantViolations: []string{"noModeSet"},
		},

		// 2. One category at a time, allowed then disallowed, one mode value per category so
		// ReadWrite, ReadOnly, and WriteOnly are each covered.
		{
			name:       "objectData requested, allowed",
			objectData: cosiapi.BucketAccessModeReadWrite,
		},
		{
			name:               "objectData requested, disallowed",
			objectData:         cosiapi.BucketAccessModeReadWrite,
			disallowObjectData: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadWrite},
			wantViolations:     []string{"objectData"},
		},
		{
			name:           "objectMetadata requested, allowed",
			objectMetadata: cosiapi.BucketAccessModeReadOnly,
		},
		{
			name:                   "objectMetadata requested, disallowed",
			objectMetadata:         cosiapi.BucketAccessModeReadOnly,
			disallowObjectMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadOnly},
			wantViolations:         []string{"objectMetadata"},
		},
		{
			name:           "bucketMetadata requested, allowed",
			bucketMetadata: cosiapi.BucketAccessModeWriteOnly,
		},
		{
			name:                   "bucketMetadata requested, disallowed",
			bucketMetadata:         cosiapi.BucketAccessModeWriteOnly,
			disallowBucketMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeWriteOnly},
			wantViolations:         []string{"bucketMetadata"},
		},

		// 3. Everything requested, nothing disallowed.
		{
			name:           "everything requested, nothing disallowed",
			objectData:     cosiapi.BucketAccessModeReadWrite,
			objectMetadata: cosiapi.BucketAccessModeReadOnly,
			bucketMetadata: cosiapi.BucketAccessModeWriteOnly,
		},

		// 4. Everything disallowed (every mode blocked in every category), but only objectData
		// is requested. objectMetadata and bucketMetadata are never checked because they weren't
		// requested, even though every mode is blocked for them too.
		{
			name:                   "everything disallowed, only objectData requested",
			objectData:             cosiapi.BucketAccessModeReadWrite,
			disallowObjectData:     allAccessModes,
			disallowObjectMetadata: allAccessModes,
			disallowBucketMetadata: allAccessModes,
			wantViolations:         []string{"objectData"},
		},

		// 5. Combination of 3 and 4: everything requested, and everything disallowed. All three
		// categories must be reported together, not just the first one found.
		{
			name:                   "everything requested, everything disallowed",
			objectData:             cosiapi.BucketAccessModeReadWrite,
			objectMetadata:         cosiapi.BucketAccessModeReadOnly,
			bucketMetadata:         cosiapi.BucketAccessModeWriteOnly,
			disallowObjectData:     allAccessModes,
			disallowObjectMetadata: allAccessModes,
			disallowBucketMetadata: allAccessModes,
			wantViolations:         []string{"objectData", "objectMetadata", "bucketMetadata"},
		},

		// 6. Combined coverage: one BucketAccessClass disallows a different subset of modes per
		// category (WriteOnly for objectData, ReadWrite and WriteOnly for objectMetadata, ReadOnly
		// for bucketMetadata), checked against three requests: fully allowed, fully disallowed,
		// and a mix of both.
		{
			name:                   "mixed disallow configuration, request fully allowed",
			objectData:             cosiapi.BucketAccessModeReadWrite,
			objectMetadata:         cosiapi.BucketAccessModeReadOnly,
			bucketMetadata:         cosiapi.BucketAccessModeReadWrite,
			disallowObjectData:     []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeWriteOnly},
			disallowObjectMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadWrite, cosiapi.BucketAccessModeWriteOnly},
			disallowBucketMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadOnly},
		},
		{
			name:                   "mixed disallow configuration, request fully disallowed",
			objectData:             cosiapi.BucketAccessModeWriteOnly,
			objectMetadata:         cosiapi.BucketAccessModeWriteOnly,
			bucketMetadata:         cosiapi.BucketAccessModeReadOnly,
			disallowObjectData:     []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeWriteOnly},
			disallowObjectMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadWrite, cosiapi.BucketAccessModeWriteOnly},
			disallowBucketMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadOnly},
			wantViolations:         []string{"objectData", "objectMetadata", "bucketMetadata"},
		},
		{
			name:                   "mixed disallow configuration, request partially disallowed",
			objectData:             cosiapi.BucketAccessModeReadWrite,
			objectMetadata:         cosiapi.BucketAccessModeWriteOnly,
			bucketMetadata:         cosiapi.BucketAccessModeReadOnly,
			disallowObjectData:     []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeWriteOnly},
			disallowObjectMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadWrite, cosiapi.BucketAccessModeWriteOnly},
			disallowBucketMetadata: []cosiapi.BucketAccessMode{cosiapi.BucketAccessModeReadOnly},
			wantViolations:         []string{"objectMetadata", "bucketMetadata"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
