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

// Package predicate defines controller-runtime Predicates that are re-used by COSI controllers to
// filter resource events before controller reconcile. COSI's split controller/sidecar architecture
// means that COSI has many reconcile filter behaviors that can be reused between components but
// where composition of smaller, individual behaviors is helpful.
package predicate

import (
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
)

// AnyCreate returns a predicate that enqueues a reconcile for any Create event.
// The predicate does not enqueue requests for any non-Create events.
func AnyCreate() predicate.Funcs {
	funcs := allFalseFuncs()
	funcs.CreateFunc = func(e event.CreateEvent) bool {
		return true
	}
	return funcs
}

// AnyDelete returns a predicate that enqueues a reconcile for any Delete event.
// The predicate does not enqueue requests for any non-Delete events.
func AnyDelete() predicate.Funcs {
	funcs := allFalseFuncs()
	funcs.DeleteFunc = func(e event.DeleteEvent) bool {
		// TODO: investigate DeleteStateUnknown to avoid reconciling nonexistent resources
		return true
	}
	return funcs
}

// AnyGeneric returns a predicate that enqueues a reconcile for any Generic event.
// The predicate does not enqueue requests for any non-Generic events.
func AnyGeneric() predicate.Funcs {
	funcs := allFalseFuncs()
	funcs.GenericFunc = func(e event.GenericEvent) bool {
		return true
	}
	return funcs
}

// GenerationChangedInUpdateOnly implements a predicate that enqueues a reconcile for Update events
// where the generation changes. For most resources, a generation change means that the resource
// `spec` has changed, ignoring metadata and status changes.
//
// The predicate does not enqueue requests for any Create/Delete/Generic events.
// This ensures that other predicates can effectively filter out undesired non-Update events.
//
// This is a modified implementation of controller-runtime's GenerationChangedPredicate{} which
// does enqueue requests for all Create/Delete/Generic events -- behavior COSI does not always want.
func GenerationChangedInUpdateOnly() predicate.Funcs {
	funcs := allFalseFuncs()
	funcs.UpdateFunc = func(e event.UpdateEvent) bool {
		return predicate.GenerationChangedPredicate{}.Update(e)
	}
	return funcs
}

// ProtectionFinalizerRemoved implements a predicate that enqueues a reconcile for Update events
// where the protection finalizer has been removed. This helps ensure that COSI always has a chance
// to re-apply the protection finalizer when it's needed.
//
// The predicate does not enqueue requests for any Create/Delete/Generic events.
// This ensures that other predicates can effectively filter out undesired non-Update events.
func ProtectionFinalizerRemoved(s *runtime.Scheme) predicate.Funcs {
	funcs := allFalseFuncs()
	funcs.UpdateFunc = func(e event.UpdateEvent) bool {
		old := e.ObjectOld
		new := e.ObjectNew

		if !new.GetDeletionTimestamp().IsZero() {
			return false // don't care if protection finalizer is missing when obj is deleting
		}

		oldHas := ctrlutil.ContainsFinalizer(old, cosiapi.ProtectionFinalizer)
		newHas := ctrlutil.ContainsFinalizer(new, cosiapi.ProtectionFinalizer)
		if oldHas && !newHas {
			logger := ctrl.Log.WithName("predicate")
			logger.Info("protection finalizer was removed from resource",
				"kind", inferKind(new, s), "namespace", new.GetNamespace(), "name", new.GetName())
			return true
		}

		return false
	}
	return funcs
}

// Makes a best-effort attempt to infer a likely Kind for the object in the schema.
// Useful because controller-runtime predicates don't have GVK info for objects, and logging object
// kind in reusable predicates can help disambiguate resources in logs.
// See: https://github.com/kubernetes-sigs/controller-runtime/issues/1735.
func inferKind(o client.Object, s *runtime.Scheme) string {
	gvks, _, err := s.ObjectKinds(o)
	if err != nil {
		return "unknown"
	}
	for _, gvk := range gvks {
		if len(gvk.Kind) > 0 && len(gvk.Version) > 0 {
			return gvk.Kind
		}
	}
	return "unknown"
}

// Returns a predicate that returns false for all Create/Update/Delete/Generic events.
// Intended to be a base building block for COSI predicates.
func allFalseFuncs() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}
