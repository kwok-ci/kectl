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

package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/kwok-ci/kectl/pkg/apis/action/v1alpha1"
	"github.com/kwok-ci/kectl/pkg/client"
	"github.com/kwok-ci/kectl/pkg/encoding"
	"github.com/kwok-ci/kectl/pkg/snapshot/handle"
	"github.com/kwok-ci/kectl/pkg/utils/heap"
	"github.com/kwok-ci/kectl/pkg/utils/yaml"
	"github.com/kwok-ci/kectl/pkg/wellknown"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/utils/clock"
)

// LoadConfig is the combination of the impersonation config
type LoadConfig struct {
	Client client.Client
	Scheme *runtime.Scheme
	Codecs serializer.CodecFactory
	Prefix string
}

// Loader loads the resources to cluster
// This way does not delete existing resources in the cluster,
// which will Handle the ownerReference so that the resources remain relative to each other
type Loader struct {
	LoadConfig

	tracksData map[schema.GroupVersionResource]map[handle.ObjectRef]json.RawMessage

	handle *handle.Handle
	clock  clock.Clock
}

// NewLoader creates a new snapshot Loader.
func NewLoader(loadConfig LoadConfig) (*Loader, error) {

	l := &Loader{
		tracksData: map[schema.GroupVersionResource]map[handle.ObjectRef]json.RawMessage{},
		LoadConfig: loadConfig,
		clock:      clock.RealClock{},
	}

	return l, nil
}

// AllowHandle allows the handle to be used
func (l *Loader) AllowHandle(ctx context.Context) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)

	handle := handle.NewHandle()

	handle.Info(ctx)
	go handle.Input(ctx)

	l.handle = handle

	return func() {
		l.handle = nil
		cancel()
	}
}

// Load loads the resources to cluster
func (l *Loader) Load(ctx context.Context, decoder *yaml.Decoder) error {

	for ctx.Err() == nil {
		obj, err := decoder.DecodeUnstructured()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			slog.Warn("failed to decode object",
				"err", err,
			)
			continue
		}

		if obj.GetKind() == handle.ResourcePatchType.Kind && obj.GetAPIVersion() == handle.ResourcePatchType.APIVersion {
			// Leave the patch to the replay function
			decoder.UndecodedUnstructured(obj)
			break
		}

		err = l.applyResource(ctx, obj)
		if err != nil {
			slog.Warn("failed to apply resource",
				"err", err,
				"kind", obj.GetKind(),
				"name", handle.KObj(obj),
			)
		}
	}

	return nil
}

// Replay replays the resources to cluster
func (l *Loader) Replay(ctx context.Context, decoder *yaml.Decoder) error {

	h := heap.NewHeap[time.Duration, *handle.ResourcePatch]()

	var dur time.Duration
	for ctx.Err() == nil {
		obj, err := decoder.DecodeUnstructured()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			slog.Warn("failed to decode object",
				"err", err,
			)
			continue
		}

		if obj.GetKind() != handle.ResourcePatchType.Kind || obj.GetAPIVersion() != handle.ResourcePatchType.APIVersion {
			slog.Warn("unexpected object",
				"kind", obj.GetKind(),
				"apiVersion", obj.GetAPIVersion(),
			)
			continue
		}

		resourcePatch, err := yaml.Convert[handle.ResourcePatch](obj)
		if err != nil {
			return err
		}

		h.Push(resourcePatch.DurationNanosecond, &resourcePatch)

		// Tolerate events that are out of order over a period of time
		if h.Len() >= 1024 {
			_, rp, _ := h.Pop()
			l.handleResourcePatch(ctx, rp, &dur)
		}
	}

	// Flush the remaining events
	for ctx.Err() == nil {
		_, rp, ok := h.Pop()
		if !ok {
			break
		}
		l.handleResourcePatch(ctx, rp, &dur)
	}

	return nil
}

func (l *Loader) handleResourcePatch(ctx context.Context, resourcePatch *handle.ResourcePatch, dur *time.Duration) {
	d := resourcePatch.DurationNanosecond - *dur
	switch {
	case d > 0:
		*dur = resourcePatch.DurationNanosecond
	case d < -1*time.Second:
		if l.handle != nil {

			sd := l.handle.SpeedDown()

			slog.Warn("Speed is too fast, speed down",
				"rate", sd,
				"over", -d,
				"current", resourcePatch.DurationNanosecond,
			)

			*dur = resourcePatch.DurationNanosecond
		}
		d = 0
	default:
		d = 0
	}

	for d > 0 {
		// Handle pause
		if l.handle != nil {
			l.handlePause(ctx)
		}

		step := min(time.Second, d)
		d -= step

		// Adjusting speed
		if step > 0 && l.handle != nil {
			step = time.Duration(float64(step) / float64(l.handle.Speed()))
		}
		if step > 0 {
			l.clock.Sleep(step)
		}
	}
	// Handle pause
	if l.handle != nil {
		l.handlePause(ctx)
	}

	start := l.clock.Now()
	l.applyResourcePatch(ctx, resourcePatch)
	past := l.clock.Since(start)
	if past > 0 {
		if l.handle != nil {
			past = time.Duration(float64(past) * float64(l.handle.Speed()))
		}
		*dur += past
	}
}

func (l *Loader) handlePause(ctx context.Context) {
	for l.handle.IsPause() {
		if err := ctx.Err(); err != nil {
			return
		}
		l.clock.Sleep(time.Second / 10)
	}
}

func (l *Loader) getData(ctx context.Context, gvr schema.GroupVersionResource, name, namespace string) ([]byte, error) {
	var dataStore []byte
	_, err := l.Client.Get(ctx, l.Prefix,
		client.WithName(name, namespace),
		client.WithGR(gvr.GroupResource()),
		client.WithResponse(func(kv *client.KeyValue) error {
			dataStore = kv.Value
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource: %w", err)
	}

	if len(dataStore) == 0 {
		return nil, fmt.Errorf("resource not found")
	}

	mediaType, err := encoding.MediaTypeFromGVR(l.Codecs, gvr)
	if err != nil {
		return nil, fmt.Errorf("failed to get media type: %w", err)
	}

	var data = dataStore
	if encoding.JSONMediaType != mediaType {
		_, data, err = encoding.Convert(l.Codecs, mediaType, encoding.JSONMediaType, dataStore)
		if err != nil {
			return nil, fmt.Errorf("failed to convert resource: %w", err)
		}
	}

	return data, nil
}

func (l *Loader) delData(ctx context.Context, gvr schema.GroupVersionResource, name, namespace string) error {
	err := l.Client.Delete(ctx, l.Prefix,
		client.WithName(name, namespace),
		client.WithGR(gvr.GroupResource()),
	)
	if err != nil {
		return fmt.Errorf("failed to delete resource: %w", err)
	}

	key := handle.KRef(namespace, name)
	delete(l.tracksData[gvr], key)
	return nil
}

func (l *Loader) putData(ctx context.Context, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	mediaType, err := encoding.MediaTypeFromGVR(l.Codecs, gvr)
	if err != nil {
		return fmt.Errorf("failed to get media type: %w", err)
	}

	obj.SetResourceVersion("")

	data, err := obj.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %w", err)
	}
	dataStore := data
	if encoding.JSONMediaType != mediaType {
		_, dataStore, err = encoding.Convert(l.Codecs, encoding.JSONMediaType, mediaType, data)
		if err != nil {
			return fmt.Errorf("failed to convert resource: %w", err)
		}
	}
	key := handle.KObj(obj)
	err = l.Client.Put(ctx, l.Prefix, dataStore,
		client.WithName(key.Name, key.Namespace),
		client.WithGR(gvr.GroupResource()),
	)
	if err != nil {
		return fmt.Errorf("failed to put resource: %w", err)
	}

	l.tracksData[gvr][key] = data
	return nil
}

func (l *Loader) patchData(ctx context.Context, gvr schema.GroupVersionResource, obj *unstructured.Unstructured, patchData []byte) error {
	mediaType, err := encoding.MediaTypeFromGVR(l.Codecs, gvr)
	if err != nil {
		return fmt.Errorf("failed to get media type: %w", err)
	}

	gvk, _, err := l.Scheme.ObjectKinds(obj)
	if err != nil {
		return fmt.Errorf("failed to lookup gvk: %w", err)
	}

	sobj, err := l.Scheme.New(gvk[0])
	if err != nil {
		return fmt.Errorf("failed to new gvk: %w", err)
	}
	statusPatchMeta, err := strategicpatch.NewPatchMetaFromStruct(sobj)
	if err != nil {
		return fmt.Errorf("failed to lookup patch meta: %w", err)
	}

	m := map[string]any{}
	err = json.Unmarshal(patchData, &m)
	if err != nil {
		return fmt.Errorf("failed to unmarshal patch data: %w", err)
	}

	dataMap, err := strategicpatch.StrategicMergeMapPatchUsingLookupPatchMeta(obj.Object, m, statusPatchMeta)
	if err != nil {
		return fmt.Errorf("failed to merge patch: %w", err)
	}

	data, err := json.Marshal(dataMap)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data: %w", err)
	}

	dataStore := data
	if encoding.JSONMediaType != mediaType {
		_, dataStore, err = encoding.Convert(l.Codecs, encoding.JSONMediaType, mediaType, data)
		if err != nil {
			return fmt.Errorf("failed to convert resource: %w", err)
		}
	}

	key := handle.KObj(obj)
	err = l.Client.Put(ctx, l.Prefix, dataStore,
		client.WithName(key.Name, key.Namespace),
		client.WithGR(gvr.GroupResource()),
	)
	if err != nil {
		return fmt.Errorf("failed to put resource: %w", err)
	}

	l.tracksData[gvr][key] = data
	return nil
}

func (l *Loader) applyResource(ctx context.Context, obj *unstructured.Unstructured) error {
	gvk := obj.GroupVersionKind()

	gr, _, ok := wellknown.CorrectGroupResource(schema.GroupResource{
		Group:    gvk.Group,
		Resource: gvk.Kind,
	})
	if !ok {
		return fmt.Errorf("failed to correct group resource for kind %s in group %s", gvk.Kind, gvk.Group)
	}

	gvr := schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: gr.Resource,
	}

	if l.tracksData[gvr] == nil {
		l.tracksData[gvr] = map[handle.ObjectRef]json.RawMessage{}
	}

	err := l.putData(ctx, gvr, obj)
	if err != nil {
		return fmt.Errorf("failed to put data: %w", err)
	}
	return nil
}

func (l *Loader) applyResourcePatch(ctx context.Context, resourcePatch *handle.ResourcePatch) {

	gvr := resourcePatch.GetTargetGroupVersionResource()

	name, namespace := resourcePatch.GetTargetName()

	if l.tracksData[gvr] == nil {
		l.tracksData[gvr] = map[handle.ObjectRef]json.RawMessage{}
	}

	key := handle.KRef(namespace, name)
	switch resourcePatch.Method {
	case v1alpha1.PatchMethodDelete:
		err := l.delData(ctx, gvr, name, namespace)
		if err != nil {
			slog.Warn("Failed to delete resource", "err", err)
			return
		}

	case v1alpha1.PatchMethodCreate:
		obj := &unstructured.Unstructured{}
		err := obj.UnmarshalJSON(resourcePatch.Template)
		if err != nil {
			slog.Warn("Failed to unmarshal resource", "err", err)
			return
		}

		err = l.putData(ctx, gvr, obj)
		if err != nil {
			slog.Warn("Failed to put resource",
				"err", err,
				"kind", obj.GetKind(),
				"name", handle.KObj(obj),
			)
			return
		}

	case v1alpha1.PatchMethodPatch:
		original := l.tracksData[gvr][key]
		if original == nil {
			origin, err := l.getData(ctx, gvr, name, namespace)
			if err != nil {
				slog.Warn("Failed to get original resource",
					"err", err,
					"gvr", gvr,
					"name", key,
				)
				return
			}
			slog.Warn("Modify a resource that is not currently created",
				"gvr", gvr,
				"name", key,
			)
			original = origin
			l.tracksData[gvr][key] = origin
		}

		obj := &unstructured.Unstructured{}
		err := obj.UnmarshalJSON(original)
		if err != nil {
			slog.Warn("Failed to unmarshal resource", "err", err)
			return
		}

		err = l.patchData(ctx, gvr, obj, resourcePatch.Template)
		if err != nil {
			slog.Warn("Failed to patch resource",
				"err", err,
				"kind", obj.GetKind(),
				"name", handle.KObj(obj),
			)
			return
		}
	}
}
