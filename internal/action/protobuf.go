package action

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/resource"
	"github.com/bclonan/kairosapi/internal/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type ProtobufConfig struct {
	Descriptor map[string]any `json:"descriptor"`
	Message    string         `json:"message"`
	Operation  string         `json:"operation"`
	Name       string         `json:"name,omitempty"`
}

func (r *Resources) Protobuf(raw json.RawMessage) (workflow.Handler, error) {
	var config ProtobufConfig
	if err := workflow.Decode(raw, &config); err != nil {
		return nil, errors.New("invalid protobuf action configuration")
	}
	if _, err := artifact.ReferenceID(config.Descriptor); err != nil {
		return nil, errors.New("protobuf needs an immutable descriptor file reference")
	}
	if len(config.Message) > 512 || !protoreflect.FullName(config.Message).IsValid() {
		return nil, errors.New("protobuf needs a fully qualified message name")
	}
	if config.Operation != "encode" && config.Operation != "decode" {
		return nil, errors.New("protobuf operation must be encode or decode")
	}
	if config.Name != "" && config.Operation != "encode" {
		return nil, errors.New("protobuf name only applies to encoding")
	}
	if config.Name == "" {
		config.Name = "message.pb"
	}
	if err := artifact.ValidateMetadata(artifact.Metadata{Name: config.Name}); err != nil {
		return nil, err
	}
	state := r.state.Load()
	if state == nil {
		return nil, errors.New("protobuf requires a configured resource store")
	}
	return func(ctx context.Context, input map[string]any) (any, error) {
		descriptor, types, err := state.loader.Message(ctx, config.Descriptor, config.Message)
		if err != nil {
			return nil, err
		}
		message := dynamicpb.NewMessage(descriptor)
		if config.Operation == "encode" {
			value, exists := input["value"]
			if !exists {
				return nil, errors.New("protobuf encoding needs input.value")
			}
			data, err := json.Marshal(value)
			if err != nil || len(data) > workflow.MaxDataBytes {
				return nil, errors.New("protobuf JSON input exceeds the action size limit")
			}
			if err := (protojson.UnmarshalOptions{Resolver: types, RecursionLimit: resource.MaxDepth}).Unmarshal(data, message); err != nil {
				return nil, errors.New("input.value does not match the protobuf message")
			}
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
			if err != nil || len(wire) > workflow.MaxDataBytes {
				return nil, errors.New("protobuf encoding exceeds the wire size limit or has missing required fields")
			}
			owner := workflow.RunID(ctx)
			if owner == "" {
				return nil, errors.New("protobuf encoding requires a tracked workflow run")
			}
			// Retry bytes are deterministic. An uncheckpointed attempt may have
			// another file ID; finalization discards its unreferenced staged file.
			meta, _, err := state.files.Put(ctx, bytes.NewReader(wire), artifact.Metadata{Name: config.Name, MediaType: "application/protobuf"}, "", "", owner)
			if err != nil {
				return nil, err
			}
			return map[string]any{"file": meta.Reference(), "message_type": config.Message}, nil
		}
		id, err := artifact.ReferenceID(input["file"])
		if err != nil {
			return nil, errors.New("protobuf decoding needs input.file as an artifact reference")
		}
		meta, reader, err := state.files.Read(ctx, id, workflow.RunID(ctx))
		if err != nil {
			return nil, err
		}
		if meta.Size > workflow.MaxDataBytes || meta.ContentEncoding != "" {
			_ = reader.Close()
			return nil, errors.New("protobuf wire input must be uncompressed and at most 256 KiB")
		}
		wire, readErr := io.ReadAll(io.LimitReader(reader, workflow.MaxDataBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if len(wire) > workflow.MaxDataBytes {
			return nil, errors.New("protobuf wire input exceeds its size limit")
		}
		if err := (proto.UnmarshalOptions{Resolver: types, RecursionLimit: resource.MaxDepth}).Unmarshal(wire, message); err != nil {
			return nil, errors.New("input.file is not a valid protobuf message")
		}
		data, err := (protojson.MarshalOptions{Resolver: types}).Marshal(message)
		if err != nil || len(data) > workflow.MaxDataBytes {
			return nil, errors.New("decoded protobuf JSON exceeds its size limit or contains unresolved types")
		}
		var value any
		if err := workflow.Decode(data, &value); err != nil {
			return nil, errors.New("decoded protobuf JSON exceeds workflow data limits")
		}
		output := map[string]any{"value": value, "message_type": config.Message}
		encoded, err := json.Marshal(output)
		if err != nil || len(encoded) > workflow.MaxDataBytes {
			return nil, errors.New("decoded protobuf output exceeds its size limit")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return output, nil
	}, nil
}
