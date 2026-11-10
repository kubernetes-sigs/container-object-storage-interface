/*
Copyright 2026 The Kubernetes Authors.

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

package conditions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetInitializing(t *testing.T) {
	// set initial conditions
	conditions := []metav1.Condition{}
	assert.True(t, SetInitializing(&conditions, 1))
	for _, condType := range allConditions {
		gotCond := meta.FindStatusCondition(conditions, condType)
		require.NotNil(t, gotCond)

		assert.Equal(t, condType, gotCond.Type)
		assert.Equal(t, metav1.ConditionUnknown, gotCond.Status)
		assert.Equal(t, "Initializing", gotCond.Reason)
		assert.Equal(t, "Initializing", gotCond.Message)
		assert.Equal(t, int64(1), gotCond.ObservedGeneration)
		assert.NotNil(t, gotCond.LastTransitionTime) // assume nonzero means time.Now()-ish
	}

	// set all times to a time we can use for expectations in later tests
	expectTime := metav1.NewTime(time.Date(2000, time.January, 1, 0, 0, 1, 0, time.UTC))
	for i := range conditions {
		conditions[i].LastTransitionTime = expectTime
	}

	testAgainNoChange := func(t *testing.T, conds []metav1.Condition, generation int64) {
		t.Run("test again no change", func(t *testing.T) {
			copy := copyConditions(conds)
			assert.False(t, SetInitializing(&copy, generation))
			assert.Equal(t, conds, copy)
		})
	}

	testAgainNoChange(t, conditions, 1)

	t.Run("generation changed", func(t *testing.T) {
		copy := copyConditions(conditions)

		assert.True(t, SetInitializing(&copy, 2))

		expectCopy := copyConditions(conditions)
		for i := range expectCopy {
			expectCopy[i].ObservedGeneration = 2
		}
		assert.ElementsMatch(t, expectCopy, copy)

		testAgainNoChange(t, copy, 2)
	})

	// test each condition individually to ensure non-initializing conditions aren't overwritten
	for _, nonInitCond := range allConditions {
		t.Run(nonInitCond+" non-initializing", func(t *testing.T) {
			copy := copyConditions(conditions)

			mod := meta.FindStatusCondition(copy, nonInitCond)
			mod.Reason = "AnythingNotInitializing"
			modCopy := *mod

			assert.True(t, SetInitializing(&copy, 3))

			for _, c := range copy {
				if c.Type == nonInitCond {
					// the condition type under test should not change, including timestamp
					assert.Equal(t, c, modCopy)
				} else {
					// all other conditions should change generation
					assert.Equal(t, c, metav1.Condition{
						Type:               c.Type,
						Status:             metav1.ConditionUnknown,
						Reason:             "Initializing",
						Message:            "Initializing",
						ObservedGeneration: 3,
						LastTransitionTime: expectTime,
					})
				}
			}

			testAgainNoChange(t, copy, 3)
		})
	}

	t.Run("all non-initializing", func(t *testing.T) {
		conds := []metav1.Condition{}
		for _, condType := range allConditions {
			conds = append(conds, metav1.Condition{
				Type:               condType,
				Status:             metav1.ConditionTrue,
				Reason:             "AnythingNotInitializing",
				Message:            "AnythingNotInitializing",
				ObservedGeneration: 88888,
				LastTransitionTime: expectTime,
			})
		}

		assert.False(t, SetInitializing(&conds, 99999)) // shouldn't update generation

		for _, condType := range allConditions {
			gotCond := meta.FindStatusCondition(conds, condType)
			require.NotNil(t, gotCond)

			assert.Equal(t, condType, gotCond.Type)
			assert.Equal(t, metav1.ConditionTrue, gotCond.Status)
			assert.Equal(t, "AnythingNotInitializing", gotCond.Reason)
			assert.Equal(t, "AnythingNotInitializing", gotCond.Message)
			assert.Equal(t, int64(88888), gotCond.ObservedGeneration)
			assert.Equal(t, expectTime, gotCond.LastTransitionTime)
		}

		testAgainNoChange(t, conds, 99999)
	})
}

func copyConditions(conditions []metav1.Condition) []metav1.Condition {
	copied := make([]metav1.Condition, len(conditions))
	copy(copied, conditions)
	return copied
}
