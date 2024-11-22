/* Copyright 2021 The Kubernetes Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package provisioner

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

func apiLogger(ctx context.Context, api string,
	req, resp interface{},
	grpcConn *grpc.ClientConn,
	apiCall grpc.UnaryInvoker,
	opts ...grpc.CallOption) error {

	if jsonReq, err := json.MarshalIndent(req, "", " "); err != nil {
		klog.InfoS("Request", "api", api, "req", string(jsonReq))
	}

	start := time.Now()
	err := apiCall(ctx, api, req, resp, grpcConn, opts...)
	end := time.Now()

	if jsonRes, err := json.MarshalIndent(resp, "", " "); err != nil {
		klog.InfoS("Response", "api", api, "elapsed", end.Sub(start), "resp", jsonRes)
	}

	return err
}
