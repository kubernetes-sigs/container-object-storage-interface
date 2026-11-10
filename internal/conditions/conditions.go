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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
)

var allConditions = []string{
	cosiapi.ProvisionedConditionType,
	cosiapi.ProvisionFailedConditionType,
	cosiapi.ResourcesValidatedConditionType,
}

// SetReconciling sets all nonexistent conditions to the reconciling state. It does not modify any
// conditions in non-reconciling states.
func SetReconciling(conditions *[]metav1.Condition) bool {
	anyChanged := false

	if *conditions == nil {
		*conditions = []metav1.Condition{}
	}

	for _, condType := range allConditions {
		newCond := metav1.Condition{
			Type:    condType,
			Status:  metav1.ConditionUnknown,
			Reason:  cosiapi.ReconcilingConditionReason,
			Message: cosiapi.ReconcilingConditionReason,
			// COSI currently does not and should not set ObservedGeneration. This may change in the
			// future, but at present the value it offers is low, and validating it across unit and
			// e2e tests adds complexity that will slow release.
			ObservedGeneration: 0,
			// LastTransitionTime handled by SetStatusCondition() below
		}

		currentCond := meta.FindStatusCondition(*conditions, condType)

		if currentCond != nil && currentCond.Reason != cosiapi.ReconcilingConditionReason {
			// exists but is not in reconciling state, DO NOT overwrite it
			continue
		}

		// May update the observed generation on any existing reconciling conditions. This ensures
		// users get some feedback to show that COSI is making progress, even if other conditions
		// aren't getting set or updated.
		changed := meta.SetStatusCondition(conditions, newCond)
		anyChanged = anyChanged || changed
	}

	return anyChanged
}
