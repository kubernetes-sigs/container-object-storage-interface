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

package main

import (
	"context"
	"flag"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"k8s.io/klog/v2"
	"sigs.k8s.io/container-object-storage-interface-api/internal/runtime"
	cosi "sigs.k8s.io/container-object-storage-interface-api/proto"
	"sigs.k8s.io/container-object-storage-interface-api/sidecar/pkg/bucket"
	"sigs.k8s.io/container-object-storage-interface-api/sidecar/pkg/bucketaccess"
	"sigs.k8s.io/container-object-storage-interface-api/sidecar/pkg/provisioner"
)

var (
	driverAddress = "unix:///var/lib/cosi/cosi.sock"
	kubeconfig    = ""
	debug         = false
)

var cmd = &cobra.Command{
	Use:           "objectstorage-sidecar",
	Short:         "provisioner that interacts with cosi drivers to manage buckets and bucketAccesses",
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return run(cmd.Context(), args)
	},
	DisableFlagsInUseLine: true,
}

func init() {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	flag.Set("alsologtostderr", "true")
	kflags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(kflags)

	persistentFlags := cmd.PersistentFlags()
	persistentFlags.AddGoFlagSet(kflags)

	stringFlag := persistentFlags.StringVarP
	boolFlag := persistentFlags.BoolVarP

	stringFlag(&kubeconfig, "kubeconfig", "", kubeconfig, "path to kubeconfig file")
	stringFlag(&driverAddress, "driver-addr", "d", driverAddress, "path to unix domain socket where driver is listening")

	boolFlag(&debug, "debug", "g", debug, "Logs all grpc requests and responses")

	viper.BindPFlags(cmd.PersistentFlags())
	cmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		if viper.IsSet(f.Name) && viper.GetString(f.Name) != "" {
			cmd.PersistentFlags().Set(f.Name, viper.GetString(f.Name))
		}
	})
}

func run(ctx context.Context, args []string) error {
	klog.V(3).InfoS("Attempting connection to driver", "address", driverAddress)
	cosiClient, err := provisioner.NewDefaultCOSIProvisionerClient(ctx, driverAddress, debug)
	if err != nil {
		return err
	}

	info, err := cosiClient.DriverGetInfo(ctx, &cosi.DriverGetInfoRequest{})
	if err != nil {
		return err
	}
	klog.V(3).InfoS("Successfully connected to driver", "name", info.Name)

	ctrl, err := runtime.NewDefaultObjectStorageController("cosi", info.Name, 40)
	if err != nil {
		return err
	}

	bl := bucket.NewBucketListener(info.Name, cosiClient)
	bal := bucketaccess.NewBucketAccessListener(info.Name, cosiClient)

	ctrl.AddBucketListener(bl)
	ctrl.AddBucketAccessListener(bal)

	return ctrl.Run(ctx)
}
