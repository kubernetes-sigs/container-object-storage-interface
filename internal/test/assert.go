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

package test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DeepCopier[T any] interface {
	DeepCopy() T
}

// AssertStatusEqualIgnoringConditions asserts that the given statuses are equal, ignoring any
// differences to conditions fields. Must pass Status structs by reference, and Statuses must
// contain the Conditions field.
func AssertStatusEqualIgnoringConditions[T any](t *testing.T, actual, expected DeepCopier[T]) {
	t.Helper()

	aCpy := actual.DeepCopy()
	eCpy := expected.DeepCopy()

	empty := reflect.ValueOf([]metav1.Condition{})

	av := reflect.ValueOf(aCpy).Elem()
	at := av.Type()
	if _, ok := at.FieldByName("Conditions"); !ok {
		panic("Conditions field not found")
	}
	reflect.ValueOf(aCpy).Elem().FieldByName("Conditions").Set(empty)

	ev := reflect.ValueOf(eCpy).Elem()
	et := ev.Type()
	if _, ok := et.FieldByName("Conditions"); !ok {
		panic("Conditions field not found")
	}
	reflect.ValueOf(eCpy).Elem().FieldByName("Conditions").Set(empty)

	assert.Equal(t, eCpy, aCpy)
}

// AssertConditionExistsAndMatches asserts that the condition with the given Type exists in the
// condition list and has fields that match the below logic:
//   - Message is validated to contain the text in expected's value
//   - LastTransitionTime is validated equal iff expected's value is nonzero
//   - LastTransitionTime is always validated to be nonzero
//   - ObservedGeneration is always expected to be zero. COSI may implement this in the future, but
//     it adds complexity that is challenging to validate while providing relatively little value.
func AssertConditionExistsAndMatches(t *testing.T, expected metav1.Condition, conditions []metav1.Condition) {
	t.Helper()

	found := meta.FindStatusCondition(conditions, expected.Type)
	require.NotNil(t, found)

	assert.Equal(t, expected.Status, found.Status)
	assert.Equal(t, expected.Reason, found.Reason)
	assert.Contains(t, found.Message, expected.Message)
	if expected.ObservedGeneration != 0 {
		panic("invalid test expectation: ObservedGeneration must be zero")
	}
	assert.False(t, found.LastTransitionTime.IsZero())
	if !expected.LastTransitionTime.IsZero() {
		assert.Equal(t, expected.LastTransitionTime, found.LastTransitionTime)
	}
}
