/*
Copyright 2024 The Kubernetes Authors.

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

package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/kwok-ci/kectl/pkg/client"
	"github.com/kwok-ci/kectl/pkg/printer"
	"github.com/kwok-ci/kectl/pkg/wellknown"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type getFlagpole struct {
	Namespace    string
	Output       string
	ChunkSize    int64
	Watch        bool
	WatchOnly    bool
	Prefix       string
	AllNamespace bool
}

func newCtlGetCommand() *cobra.Command {
	flags := &getFlagpole{}

	cmd := &cobra.Command{
		Args:  cobra.RangeArgs(0, 2),
		Use:   "get [resource] [name]",
		Short: "Gets the resource of k8s in etcd",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdclient, err := clientFromCmd(cmd)
			if err != nil {
				return err
			}
			err = getCommand(cmd.Context(), etcdclient, flags, args)

			if err != nil {
				return fmt.Errorf("%v: %w", args, err)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&flags.Output, "output", "o", "key", "output format. One of: (json, yaml, key, none).")
	cmd.Flags().StringVarP(&flags.Namespace, "namespace", "n", "", "namespace of resource")
	cmd.Flags().BoolVarP(&flags.Watch, "watch", "w", false, "after listing/getting the requested object, watch for changes")
	cmd.Flags().BoolVar(&flags.WatchOnly, "watch-only", false, "watch for changes to the requested object(s), without listing/getting first")
	cmd.Flags().Int64Var(&flags.ChunkSize, "chunk-size", 500, "chunk size of the list pager")
	cmd.Flags().StringVar(&flags.Prefix, "prefix", "/registry", "prefix to prepend to the resource")
	cmd.Flags().BoolVarP(&flags.AllNamespace, "all-namespace", "A", false, "all namespace")

	return cmd
}

func getCommand(ctx context.Context, etcdclient client.Client, flags *getFlagpole, args []string) error {
	var targetGr schema.GroupResource
	var targetName string
	var targetNamespace string
	if len(args) != 0 {
		// TODO: Support get information from CRD
		//       Support short name
		//       Check for namespaced

		gr := schema.ParseGroupResource(args[0])
		if gr.Empty() {
			return fmt.Errorf("invalid resource %q", args[0])
		}
		targetGr = gr
		targetNamespace = flags.Namespace
		if len(args) >= 2 {
			targetName = args[1]
		}

		if correctGr, namespaced, found := wellknown.CorrectGroupResource(gr); found {
			targetGr = correctGr
			if !namespaced || flags.AllNamespace {
				targetNamespace = ""
			} else if flags.Namespace == "" {
				targetNamespace = "default"
			}
		}
	}

	opOpts := []client.OpOption{
		client.WithName(targetName, targetNamespace),
		client.WithGR(targetGr),
		client.WithPageLimit(flags.ChunkSize),
	}

	p, err := printer.NewPrinter(os.Stdout, flags.Output)
	if err != nil {
		return err
	}

	var count int
	if flags.Output == "key" {
		opOpts = append(opOpts,
			client.WithResponse(func(kv *client.KeyValue) error {
				count++
				return p.Print(kv)
			}),
			client.WithKeysOnly(),
		)
	} else {
		opOpts = append(opOpts,
			client.WithResponse(p.Print),
		)
	}

	if flags.Watch || flags.WatchOnly {
		var rev int64
		if !flags.WatchOnly {
			rev, err = etcdclient.Get(ctx, flags.Prefix,
				opOpts...,
			)
			if err != nil {
				return err
			}
		}

		opOpts = append(opOpts, client.WithRevision(rev))

		err = etcdclient.Watch(ctx, flags.Prefix,
			opOpts...,
		)
		if err != nil {
			return err
		}
	} else {
		_, err = etcdclient.Get(ctx, flags.Prefix,
			opOpts...,
		)
		if err != nil {
			return err
		}

		if flags.Output == "key" {
			fmt.Fprintf(os.Stderr, "get %d keys\n", count)
		}
	}
	return nil
}
