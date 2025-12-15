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

package v1alpha2

/*
This file for types that don't belong to any COSI resource specifically.
Use `<type>_types.go` files for resource-specific types.
*/

import (
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TimestampedError contains an error message with timestamp.
type TimestampedError struct {
	// time is the timestamp when the error was encountered.
	// +optional
	Time *meta.Time `json:"time,omitempty" protobuf:"bytes,1,opt,name=time"`

	// message is a string detailing the encountered error.
	// NOTE: message will be logged, and it should not contain sensitive information.
	// +optional
	Message *string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
}

// NewTimestampedError creates a TimestampedError with the given info.
// Result fields will be nil for zero-value inputs.
func NewTimestampedError(t time.Time, message string) *TimestampedError {
	tp := &meta.Time{Time: t}
	if tp.IsZero() {
		tp = nil
	}
	mp := &message
	if *mp == "" {
		mp = nil
	}
	return &TimestampedError{
		Time:    tp,
		Message: mp,
	}
}
