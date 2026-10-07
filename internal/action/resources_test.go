package action_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/orchestrator"
	"github.com/bclonan/kairosapi/internal/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func resourceService(t *testing.T) (*orchestrator.Service, *action.Resources) {
	t.Helper()
	actions := action.NewResources()
	service, err := orchestrator.Open(orchestrator.Options{Path: filepath.Join(t.TempDir(), "kairos.db"), Workers: 2, MaxRuns: 2, QueueSize: 4, MaxRecords: 100, Retention: time.Minute, Timeout: 5 * time.Second, ConfigureFiles: actions.SetFiles}, workflow.Registry{"dictionary": actions.Dictionary, "protobuf": actions.Protobuf, "value": action.Value}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	return service, actions
}

func saveResource(t *testing.T, service *orchestrator.Service, data []byte) artifact.Metadata {
	t.Helper()
	meta, _, err := service.Files().Put(context.Background(), bytes.NewReader(data), artifact.Metadata{Name: "resource.bin"}, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func codecDescriptor(t *testing.T) []byte {
	t.Helper()
	fixture, err := os.ReadFile(filepath.Join("..", "..", "docs", "examples", "resources", "person.desc"))
	if err != nil {
		t.Fatal(err)
	}
	var saved descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(fixture, &saved); err != nil {
		t.Fatal(err)
	}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{Name: proto.String("person.proto"), Package: proto.String("demo"), Syntax: proto.String("proto3"), MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Person"), Field: []*descriptorpb.FieldDescriptorProto{
		{Name: proto.String("name"), JsonName: proto.String("name"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
		{Name: proto.String("customer_id"), JsonName: proto.String("customerId"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()},
		{Name: proto.String("payload"), JsonName: proto.String("payload"), Number: proto.Int32(3), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
	}}}}}}
	if !proto.Equal(&saved, set) {
		t.Fatal("committed descriptor fixture differs from the expected person schema")
	}
	return fixture
}

func configJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func runResource(t *testing.T, service *orchestrator.Service, definition workflow.Definition) orchestrator.Run {
	t.Helper()
	if _, err := service.Publish(definition); err != nil {
		t.Fatal(err)
	}
	run, _, err := service.Submit(orchestrator.Request{WorkflowID: definition.ID}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	run, err = service.Wait(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestDictionaryAndProtobufWorkflowUsesRealWireBytes(t *testing.T) {
	service, _ := resourceService(t)
	dictionary := saveResource(t, service, []byte(`{"people":{"ada":{"name":"Ada","customerId":"9007199254740993","payload":"AAH/"}}}`))
	descriptor := saveResource(t, service, codecDescriptor(t))
	definition := workflow.Definition{ID: "typed_chain", Version: 1, Steps: []workflow.Step{
		{ID: "lookup", Action: "dictionary", Config: configJSON(t, action.DictionaryConfig{Resource: dictionary.Reference(), Pointer: "/people"}), Input: map[string]any{"key": "ada"}},
		{ID: "encode", Action: "protobuf", DependsOn: []string{"lookup"}, Config: configJSON(t, action.ProtobufConfig{Descriptor: descriptor.Reference(), Message: "demo.Person", Operation: "encode", Name: "person.pb"}), Input: map[string]any{"value": map[string]any{"$ref": "/steps/lookup/value"}}},
		{ID: "decode", Action: "protobuf", DependsOn: []string{"encode"}, Config: configJSON(t, action.ProtobufConfig{Descriptor: descriptor.Reference(), Message: "demo.Person", Operation: "decode"}), Input: map[string]any{"file": map[string]any{"$ref": "/steps/encode/file"}}},
	}, Output: map[string]any{"person": map[string]any{"$ref": "/steps/decode/value"}, "file": map[string]any{"$ref": "/steps/encode/file"}}}
	run := runResource(t, service, definition)
	if run.State != "succeeded" {
		t.Fatalf("%+v", run)
	}
	output := run.Output.(map[string]any)
	person := output["person"].(map[string]any)
	if person["name"] != "Ada" || person["customerId"] != "9007199254740993" || person["payload"] != "AAH/" {
		t.Fatalf("protobuf changed JSON values: %v", person)
	}
	id, err := artifact.ReferenceID(output["file"])
	if err != nil {
		t.Fatal(err)
	}
	meta, reader, err := service.Files().Read(context.Background(), id, "")
	if err != nil {
		t.Fatal(err)
	}
	wire, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	// These are actual protobuf tags and varints, not a JSON or base64 wrapper.
	expected := []byte{0x0a, 0x03, 'A', 'd', 'a', 0x10, 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x10, 0x1a, 0x03, 0, 1, 255}
	if !bytes.Equal(wire, expected) || meta.Name != "person.pb" || meta.MediaType != "application/protobuf" {
		t.Fatalf("wire=%x meta=%+v", wire, meta)
	}
	for _, id := range []string{dictionary.ID, descriptor.ID, meta.ID} {
		if err := service.Files().Delete(id); !errors.Is(err, artifact.ErrReferenced) {
			t.Fatalf("referenced file %s could be removed: %v", id, err)
		}
	}
}

func TestResourcePreparationAndExecutionRejectInvalidTypes(t *testing.T) {
	service, actions := resourceService(t)
	descriptor := saveResource(t, service, codecDescriptor(t))
	invalid := []action.ProtobufConfig{
		{Descriptor: descriptor.Reference(), Message: "demo.Person", Operation: "unknown"},
		{Descriptor: descriptor.Reference(), Message: "../demo.Person", Operation: "encode"},
		{Descriptor: descriptor.Reference(), Message: "demo.Person", Operation: "decode", Name: "unused.pb"},
		{Descriptor: descriptor.Reference(), Message: "demo.Person", Operation: "encode", Name: "../bad.pb"},
	}
	for _, config := range invalid {
		if _, err := actions.Protobuf(configJSON(t, config)); err == nil {
			t.Fatalf("accepted invalid config %+v", config)
		}
	}
	if err := actions.SetFiles(service.Files()); err == nil {
		t.Fatal("resource store was replaced")
	}
	if _, err := action.NewResources().Protobuf(configJSON(t, action.ProtobufConfig{Descriptor: descriptor.Reference(), Message: "demo.Person", Operation: "encode"})); err == nil {
		t.Fatal("unconfigured file store accepted")
	}
	for _, scenario := range []string{"unknown_field", "wrong_type", "missing_value", "missing_message", "malformed_wire"} {
		t.Run(scenario, func(t *testing.T) {
			config := action.ProtobufConfig{Descriptor: descriptor.Reference(), Message: "demo.Person", Operation: "encode"}
			input := map[string]any{"value": map[string]any{"name": "Ada"}}
			switch scenario {
			case "unknown_field":
				input["value"] = map[string]any{"surprise": true}
			case "wrong_type":
				input["value"] = map[string]any{"customerId": "not-an-int64"}
			case "missing_value":
				input = map[string]any{}
			case "missing_message":
				config.Message = "demo.Missing"
			case "malformed_wire":
				config.Operation = "decode"
				input = map[string]any{"file": saveResource(t, service, []byte{0x0a, 255}).Reference()}
			}
			definition := workflow.Definition{ID: scenario, Version: 1, Steps: []workflow.Step{{ID: "codec", Action: "protobuf", Config: configJSON(t, config), Input: input}}}
			if run := runResource(t, service, definition); run.State != "failed" {
				t.Fatalf("invalid protobuf input succeeded: %+v", run)
			}
		})
	}
}

func TestDictionaryEscapedKeysAndOutputLimits(t *testing.T) {
	service, actions := resourceService(t)
	dictionary := saveResource(t, service, []byte(`{"entries":{"a/b~c":{"ok":true},"large":"`+strings.Repeat("x", workflow.MaxDataBytes)+`"}}`))
	handler, err := actions.Dictionary(configJSON(t, action.DictionaryConfig{Resource: dictionary.Reference(), Pointer: "/entries"}))
	if err != nil {
		t.Fatal(err)
	}
	result, err := handler(context.Background(), map[string]any{"key": "a/b~c"})
	if err != nil || result.(map[string]any)["value"].(map[string]any)["ok"] != true {
		t.Fatalf("%v %v", result, err)
	}
	for _, key := range []any{"missing", "large", 5} {
		if _, err := handler(context.Background(), map[string]any{"key": key}); err == nil {
			t.Fatalf("invalid selection was accepted: %v", key)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := handler(ctx, map[string]any{"key": "a/b~c"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
