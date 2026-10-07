// Package resource resolves immutable file references into bounded, reusable data.
// It never fetches URLs or opens caller-supplied filesystem paths.
package resource

import (
	"container/list"
	"context"
	"errors"
	"io"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const MaxBytes = 1 << 20
const MaxEntries = 32
const MaxCacheBytes = 8 << 20
const MaxDescriptorFiles = 64
const MaxDescriptorNodes = 8192
const MaxDepth = 32

var ErrInvalid = errors.New("invalid typed resource")
var ErrLimit = errors.New("typed resource exceeds its size or complexity limit")

type entry struct {
	key   string
	size  int
	value any
}

type descriptors struct {
	files *protoregistry.Files
	types *dynamicpb.Types
}

// Loader shares immutable descriptor graphs and JSON dictionaries between runs.
// Cache accounting measures source bytes, not exact Go heap allocation. Graph
// complexity and entry limits also bound decoded objects. Only one miss loads at
// a time, and callers can cancel while waiting for the loader.
type Loader struct {
	files *artifact.Store
	gate  chan struct{}
	cache map[string]*list.Element
	lru   *list.List
	bytes int
}

func New(files *artifact.Store) (*Loader, error) {
	if files == nil {
		return nil, ErrInvalid
	}
	return &Loader{files: files, gate: make(chan struct{}, 1), cache: map[string]*list.Element{}, lru: list.New()}, nil
}

func (l *Loader) load(ctx context.Context, reference map[string]any, kind string) (any, error) {
	id, err := artifact.ReferenceID(reference)
	if err != nil {
		return nil, ErrInvalid
	}
	select {
	case l.gate <- struct{}{}:
		defer func() { <-l.gate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Check publication even for cached content. A cache entry must not make a
	// deleted file or another run's staged file accessible.
	meta, err := l.files.Get(id)
	if err != nil {
		return nil, err
	}
	if meta.Size > MaxBytes {
		return nil, ErrLimit
	}
	if meta.ContentEncoding != "" {
		return nil, errors.New("typed resources require uncompressed bytes")
	}
	key := kind + ":" + id + ":" + meta.SHA256
	if hit := l.cache[key]; hit != nil {
		l.lru.MoveToFront(hit)
		return hit.Value.(entry).value, nil
	}
	_, reader, err := l.files.Read(ctx, id, "")
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, MaxBytes+1))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > MaxBytes {
		return nil, ErrLimit
	}
	var value any
	switch kind {
	case "dictionary":
		var dictionary map[string]any
		if err := workflow.Decode(data, &dictionary); err != nil || dictionary == nil {
			return nil, ErrInvalid
		}
		value = dictionary
	case "protobuf":
		value, err = parseDescriptors(data)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for l.lru.Len() >= MaxEntries || l.bytes+len(data) > MaxCacheBytes {
		oldest := l.lru.Back()
		previous := oldest.Value.(entry)
		delete(l.cache, previous.key)
		l.bytes -= previous.size
		l.lru.Remove(oldest)
	}
	l.cache[key] = l.lru.PushFront(entry{key: key, size: len(data), value: value})
	l.bytes += len(data)
	return value, nil
}

// Lookup returns an independent copy of a dictionary value at a JSON pointer.
// The caller cannot mutate another run's cached dictionary.
func (l *Loader) Lookup(ctx context.Context, reference map[string]any, pointer string) (any, error) {
	if err := workflow.ValidatePointer(pointer); err != nil {
		return nil, err
	}
	dictionary, err := l.load(ctx, reference, "dictionary")
	if err != nil {
		return nil, err
	}
	value, err := workflow.Lookup(dictionary, pointer)
	if err != nil {
		return nil, err
	}
	return clone(value), ctx.Err()
}

func clone(value any) any {
	switch value := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, child := range value {
			out[key] = clone(child)
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, child := range value {
			out[i] = clone(child)
		}
		return out
	default:
		return value
	}
}

// Message resolves a fully qualified message name from a FileDescriptorSet.
// All imports must be in that set. No process-global or remote resolver is used.
// Returned descriptors and the type resolver are immutable and safe to share.
// Every invocation must construct its own mutable dynamic message.
func (l *Loader) Message(ctx context.Context, reference map[string]any, name string) (protoreflect.MessageDescriptor, *dynamicpb.Types, error) {
	if !protoreflect.FullName(name).IsValid() {
		return nil, nil, ErrInvalid
	}
	value, err := l.load(ctx, reference, "protobuf")
	if err != nil {
		return nil, nil, err
	}
	set := value.(*descriptors)
	descriptor, err := set.files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, nil, errors.New("protobuf message is absent from the descriptor set")
	}
	message, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, errors.New("protobuf descriptor is not a message")
	}
	return message, set.types, nil
}

func parseDescriptors(data []byte) (*descriptors, error) {
	set := &descriptorpb.FileDescriptorSet{}
	if err := (proto.UnmarshalOptions{RecursionLimit: MaxDepth}).Unmarshal(data, set); err != nil {
		return nil, ErrInvalid
	}
	if len(set.File) == 0 || len(set.File) > MaxDescriptorFiles {
		return nil, ErrLimit
	}
	nodes := 0
	var visit func(protoreflect.Message, int) bool
	visit = func(message protoreflect.Message, depth int) bool {
		nodes++
		if nodes > MaxDescriptorNodes || depth > MaxDepth {
			return false
		}
		valid := true
		message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.Kind() != protoreflect.MessageKind {
				return true
			}
			if field.IsList() {
				for i := 0; i < value.List().Len(); i++ {
					if !visit(value.List().Get(i).Message(), depth+1) {
						valid = false
						return false
					}
				}
			} else if !visit(value.Message(), depth+1) {
				valid = false
			}
			return valid
		})
		return valid
	}
	if !visit(set.ProtoReflect(), 0) {
		return nil, ErrLimit
	}
	files, err := protodesc.NewFiles(set)
	if err != nil {
		return nil, errors.New("invalid protobuf descriptor set or missing imports")
	}
	return &descriptors{files: files, types: dynamicpb.NewTypes(files)}, nil
}
