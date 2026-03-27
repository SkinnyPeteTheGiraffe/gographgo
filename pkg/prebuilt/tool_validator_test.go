package prebuilt_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/prebuilt"
)

func TestValidationNode_Validate(t *testing.T) {
	node := prebuilt.NewValidationNode(map[string]prebuilt.ToolArgsValidator{
		"sum": func(args map[string]any) error {
			if _, ok := args["a"].(int); !ok {
				return fmt.Errorf("a must be int")
			}
			if _, ok := args["b"].(int); !ok {
				return fmt.Errorf("b must be int")
			}
			return nil
		},
	}, nil)

	got, err := node.Validate(context.Background(), []prebuilt.ToolCall{
		{ID: "1", Name: "sum", Args: map[string]any{"a": 1, "b": 2}},
		{ID: "2", Name: "sum", Args: map[string]any{"a": "bad", "b": 2}},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Status != "ok" || got[0].Content != `{"a":1,"b":2}` {
		t.Fatalf("first result = %+v", got[0])
	}
	if got[1].Status != "error" {
		t.Fatalf("second status = %q, want error", got[1].Status)
	}
}

func TestValidationNode_Validate_NoValidator(t *testing.T) {
	node := prebuilt.NewValidationNode(nil, nil)

	got, err := node.Validate(context.Background(), []prebuilt.ToolCall{{
		ID:   "missing",
		Name: "unknown",
		Args: map[string]any{"x": 1},
	}})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Status != "error" {
		t.Fatalf("status = %q, want error", got[0].Status)
	}
	if !strings.Contains(got[0].Content, `No validator registered for tool "unknown"`) {
		t.Fatalf("content = %q, want missing-validator message", got[0].Content)
	}
}

func TestValidationNode_Validate_ContextCanceled(t *testing.T) {
	node := prebuilt.NewValidationNode(map[string]prebuilt.ToolArgsValidator{
		"sum": func(_ map[string]any) error { return nil },
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := node.Validate(ctx, []prebuilt.ToolCall{{
		ID:   "1",
		Name: "sum",
		Args: map[string]any{"a": 1},
	}})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if err != context.Canceled {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
}

func TestValidationNode_Validate_MarshalError(t *testing.T) {
	node := prebuilt.NewValidationNode(map[string]prebuilt.ToolArgsValidator{
		"sum": func(_ map[string]any) error { return nil },
	}, nil)

	_, err := node.Validate(context.Background(), []prebuilt.ToolCall{{
		ID:   "1",
		Name: "sum",
		Args: map[string]any{"bad": func() {}},
	}})
	if err == nil {
		t.Fatal("expected json marshal error")
	}
	if !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("err = %v, want unsupported type", err)
	}
}

func TestValidationNodeFromSchemas_StructAndProviderSources(t *testing.T) {
	node, err := prebuilt.NewValidationNodeFromSchemas([]any{
		prebuilt.ValidationSchemaSpec{Name: "sum", Schema: sumSchema{}},
		providerTool{name: "mul", schema: sumSchema{}},
	}, nil)
	if err != nil {
		t.Fatalf("NewValidationNodeFromSchemas: %v", err)
	}

	got, err := node.Validate(context.Background(), []prebuilt.ToolCall{
		{ID: "1", Name: "sum", Args: map[string]any{"a": 2, "b": 3}},
		{ID: "2", Name: "mul", Args: map[string]any{"a": "bad", "b": 3}},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Status != "ok" {
		t.Fatalf("first status = %q, want ok", got[0].Status)
	}
	if got[1].Status != "error" {
		t.Fatalf("second status = %q, want error", got[1].Status)
	}
	if !strings.Contains(got[1].Content, "a") {
		t.Fatalf("second content = %q, want field detail", got[1].Content)
	}
}

func TestValidationNodeFromSchemas_CustomModelValidation(t *testing.T) {
	node, err := prebuilt.NewValidationNodeFromSchemas([]any{
		prebuilt.ValidationSchemaSpec{Name: "strict", Schema: strictSchema{}},
	}, nil)
	if err != nil {
		t.Fatalf("NewValidationNodeFromSchemas: %v", err)
	}

	got, err := node.Validate(context.Background(), []prebuilt.ToolCall{
		{ID: "1", Name: "strict", Args: map[string]any{"a": 7}},
		{ID: "2", Name: "strict", Args: map[string]any{"a": 37}},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got[0].Status != "error" {
		t.Fatalf("first status = %q, want error", got[0].Status)
	}
	if got[1].Status != "ok" {
		t.Fatalf("second status = %q, want ok", got[1].Status)
	}
}

func TestValidationNodeFromSchemas_FormatterReceivesSchema(t *testing.T) {
	var seenSchema string
	node, err := prebuilt.NewValidationNodeFromSchemas([]any{
		prebuilt.ValidationSchemaSpec{Name: "sum", Schema: sumSchema{}},
	}, func(err error, _ prebuilt.ToolCall, schema prebuilt.ToolValidationSchema) string {
		seenSchema = schema.Name
		return err.Error()
	})
	if err != nil {
		t.Fatalf("NewValidationNodeFromSchemas: %v", err)
	}

	_, err = node.Validate(context.Background(), []prebuilt.ToolCall{
		{ID: "1", Name: "sum", Args: map[string]any{"a": "bad"}},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if seenSchema != "sum" {
		t.Fatalf("schema = %q, want sum", seenSchema)
	}
}

func TestBuildValidationSchemas_FunctionSource(t *testing.T) {
	schemas, err := prebuilt.BuildValidationSchemas([]any{prebuilt.ValidationSchemaSpec{Name: "sum", Schema: func(args map[string]any) error {
		if _, ok := args["a"].(int); !ok {
			return fmt.Errorf("a must be int")
		}
		return nil
	}}})
	if err != nil {
		t.Fatalf("BuildValidationSchemas: %v", err)
	}
	validator := schemas["sum"].Validator
	if err := validator(map[string]any{"a": 1}); err != nil {
		t.Fatalf("validator: %v", err)
	}
	if err := validator(map[string]any{"a": "bad"}); err == nil {
		t.Fatal("expected validation failure")
	}
}

type sumSchema struct {
	A int `json:"a"`
	B int `json:"b"`
}

type strictSchema struct {
	A int `json:"a"`
}

func (s strictSchema) Validate() error {
	if s.A != 37 {
		return fmt.Errorf("only 37 is allowed")
	}
	return nil
}

type providerTool struct {
	schema any
	name   string
}

func (t providerTool) Name() string {
	return t.name
}

func (t providerTool) Invoke(_ context.Context, _ map[string]any) (any, error) {
	return nil, nil
}

func (t providerTool) ArgsSchema() any {
	return t.schema
}
