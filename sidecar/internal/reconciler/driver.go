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
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/validation"

	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

// DriverInfo contains critical info about the paired driver that is needed by all reconcilers
type DriverInfo struct {
	name               string
	supportedProtocols []cosiproto.ObjectProtocol_Type

	provisionerClient cosiproto.ProvisionerClient
}

// GetName returns the name of the driver
func (d *DriverInfo) GetName() string {
	return d.name
}

// ValidateAndSetDriverConnectionInfo parses and validates the driver's reported info and returns a
// struct needed by reconcilers to connect with the driver.
func ValidateAndSetDriverConnectionInfo(
	driverReportedInfo *cosiproto.DriverGetInfoResponse,
	conn grpc.ClientConnInterface,
) (*DriverInfo, error) {
	err := validateDriverName(driverReportedInfo.Name)
	if err != nil {
		return nil, fmt.Errorf("driver name is invalid: %w", err)
	}

	parsedProtocols, err := validateAndParseProtocols(driverReportedInfo.GetSupportedProtocols())
	if err != nil {
		return nil, fmt.Errorf("supported protocols list is invalid: %w", err)
	}

	di := &DriverInfo{
		name:               driverReportedInfo.Name,
		supportedProtocols: parsedProtocols,

		provisionerClient: cosiproto.NewProvisionerClient(conn),
	}
	return di, nil
}

// validate driver name matches requirements
func validateDriverName(n string) error {
	allErrs := []string{}

	if len(n) > 63 {
		allErrs = append(allErrs, fmt.Sprintf("must be no more than 63 characters: length=%d", len(n)))
	}

	// An RFC-1035 name is a series of valid labels, optionally separated by `.` chars
	labels := strings.Split(n, ".")
	for _, l := range labels {
		errs := validation.IsDNS1035Label(l)
		if len(errs) > 0 {
			comb := fmt.Sprintf("%q is not a valid RFC-1035 label: %v", l, errs)
			allErrs = append(allErrs, comb)
		}
	}

	if len(allErrs) > 0 {
		return fmt.Errorf("driver name %q is invalid: %v", n, allErrs)
	}
	return nil
}

// parse object protocols into format runtime format
func validateAndParseProtocols(objProtocols []*cosiproto.ObjectProtocol) ([]cosiproto.ObjectProtocol_Type, error) {
	out := []cosiproto.ObjectProtocol_Type{}
	seen := map[cosiproto.ObjectProtocol_Type]struct{}{} // used to deduplicate the input list

	if len(objProtocols) == 0 {
		return []cosiproto.ObjectProtocol_Type{}, fmt.Errorf("at least one object protocol must be supported")
	}

	for _, op := range objProtocols {
		// assume we never need to check if a list entry is nil
		t := op.Type
		if t == cosiproto.ObjectProtocol_UNKNOWN {
			return []cosiproto.ObjectProtocol_Type{}, fmt.Errorf("object protocol %q is unknown", op.String())
		}
		if _, ok := seen[t]; ok {
			return []cosiproto.ObjectProtocol_Type{}, fmt.Errorf("object protocol")
		}
		if _, ok := seen[t]; !ok {
			out = append(out, t)
			seen[t] = struct{}{} // don't add this proto to the output list again
		}
	}

	return out, nil
}
