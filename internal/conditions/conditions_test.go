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
	assert.True(t, SetReconciling(&conditions))
	for _, condType := range allConditions {
		gotCond := meta.FindStatusCondition(conditions, condType)
		require.NotNil(t, gotCond)

		assert.Equal(t, condType, gotCond.Type)
		assert.Equal(t, metav1.ConditionUnknown, gotCond.Status)
		assert.Equal(t, "Reconciling", gotCond.Reason)
		assert.Equal(t, "Reconciling", gotCond.Message)
		assert.Zero(t, gotCond.ObservedGeneration)
		assert.NotNil(t, gotCond.LastTransitionTime) // assume nonzero means time.Now()-ish
	}

	// set all times to a time we can use for expectations in later tests
	expectTime := metav1.NewTime(time.Date(2000, time.January, 1, 0, 0, 1, 0, time.UTC))
	for i := range conditions {
		conditions[i].LastTransitionTime = expectTime
	}

	testAgainNoChange := func(t *testing.T, conds []metav1.Condition) {
		t.Run("test again no change", func(t *testing.T) {
			copy := copyConditions(conds)
			assert.False(t, SetReconciling(&copy))
			assert.Equal(t, conds, copy)
		})
	}

	testAgainNoChange(t, conditions)

	// test each condition individually to ensure non-initializing conditions aren't overwritten
	for _, nonInitCond := range allConditions {
		t.Run(nonInitCond+" non-initializing", func(t *testing.T) {
			copy := copyConditions(conditions)

			mod := meta.FindStatusCondition(copy, nonInitCond)
			mod.Reason = "AnythingNotInitializing"
			modCopy := *mod

			assert.False(t, SetReconciling(&copy))

			for _, c := range copy {
				if c.Type == nonInitCond {
					// the condition type under test should not change, including timestamp
					assert.Equal(t, c, modCopy)
				} else {
					// all other conditions should change generation
					assert.Equal(t, c, metav1.Condition{
						Type:               c.Type,
						Status:             metav1.ConditionUnknown,
						Reason:             "Reconciling",
						Message:            "Reconciling",
						ObservedGeneration: 0,
						LastTransitionTime: expectTime,
					})
				}
			}

			testAgainNoChange(t, copy)
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
				LastTransitionTime: expectTime,
			})
		}

		assert.False(t, SetReconciling(&conds)) // shouldn't update generation

		for _, condType := range allConditions {
			gotCond := meta.FindStatusCondition(conds, condType)
			require.NotNil(t, gotCond)

			assert.Equal(t, condType, gotCond.Type)
			assert.Equal(t, metav1.ConditionTrue, gotCond.Status)
			assert.Equal(t, "AnythingNotInitializing", gotCond.Reason)
			assert.Equal(t, "AnythingNotInitializing", gotCond.Message)
			assert.Zero(t, gotCond.ObservedGeneration)
			assert.Equal(t, expectTime, gotCond.LastTransitionTime)
		}

		testAgainNoChange(t, conds)
	})
}

func copyConditions(conditions []metav1.Condition) []metav1.Condition {
	copied := make([]metav1.Condition, len(conditions))
	copy(copied, conditions)
	return copied
}
