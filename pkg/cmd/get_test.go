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

package cmd

import (
	"context"
	"testing"

	"github.com/kwok-ci/kectl/pkg/client"
)

type watchModeClient struct {
	client.Client
	gets    int
	watches int
}

func (c *watchModeClient) Get(context.Context, string, ...client.OpOption) (int64, error) {
	c.gets++
	return 42, nil
}

func (c *watchModeClient) Watch(context.Context, string, ...client.OpOption) error {
	c.watches++
	return nil
}

func TestGetWatchModes(t *testing.T) {
	for _, tt := range []struct {
		name      string
		watch     bool
		watchOnly bool
		gets      int
		watches   int
	}{
		{name: "get", gets: 1},
		{name: "watch", watch: true, gets: 1, watches: 1},
		{name: "watch-only", watchOnly: true, watches: 1},
		{name: "both watch flags", watch: true, watchOnly: true, watches: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &watchModeClient{}
			flags := &getFlagpole{Output: "none", Prefix: "/registry", Watch: tt.watch, WatchOnly: tt.watchOnly}
			if err := getCommand(context.Background(), c, flags, []string{"services", "kubernetes"}); err != nil {
				t.Fatal(err)
			}
			if c.gets != tt.gets || c.watches != tt.watches {
				t.Errorf("get/watch calls = %d/%d, want %d/%d", c.gets, c.watches, tt.gets, tt.watches)
			}
		})
	}
}
