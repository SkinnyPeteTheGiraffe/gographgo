package prebuilt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

// ToolArgsValidator validates tool arguments before execution.
type ToolArgsValidator func(args map[string]any) error

// ToolArgsSchemaModel supports model-level validation hooks after arg decoding.
type ToolArgsSchemaModel interface {
	Validate() error
}

// ToolArgsSchemaProvider exposes a tool-name + schema pair.
type ToolArgsSchemaProvider interface {
	Name() string
	ArgsSchema() any
}

// ValidationSchemaSpec binds a schema source to a tool name.
//
// Supported schema values:
//   - `ToolArgsValidator`
//   - struct values, pointers to structs, or `reflect.Type` for struct types
//   - functions with one struct parameter (used as a schema shape)
type ValidationSchemaSpec struct {
	Schema any
	Name   string
}

// ToolValidationSchema is the compiled schema entry for one tool.
type ToolValidationSchema struct {
	Validator ToolArgsValidator
	Source    any
	Name      string
}

// ValidationErrorFormatter turns validation failures into message content.
type ValidationErrorFormatter func(err error, call ToolCall, schema ToolValidationSchema) string

// DefaultValidationErrorFormatter provides the default reprompt-style message.
func DefaultValidationErrorFormatter(err error, _ ToolCall, _ ToolValidationSchema) string {
	return fmt.Sprintf("%v\n\nRespond after fixing all validation errors.", err)
}

// ValidationNode validates tool calls and emits ToolMessage results.
// It does not execute tools.
type ValidationNode struct {
	schemasByName map[string]ToolValidationSchema
	formatError   ValidationErrorFormatter
}

// NewValidationNode creates a validation node.
func NewValidationNode(validators map[string]ToolArgsValidator, formatter ValidationErrorFormatter) *ValidationNode {
	out := make(map[string]ToolValidationSchema, len(validators))
	for name, v := range validators {
		out[name] = ToolValidationSchema{Name: name, Source: "validator", Validator: v}
	}
	if formatter == nil {
		formatter = DefaultValidationErrorFormatter
	}
	return &ValidationNode{schemasByName: out, formatError: formatter}
}

// NewValidationNodeFromSchemas creates a validation node from schema sources.
//
// Each schema source may be one of:
//   - `ValidationSchemaSpec`
//   - `ToolArgsSchemaProvider`
//   - struct value / pointer / `reflect.Type`
//   - `ToolArgsValidator` (name inferred from function symbol)
//   - function with one struct parameter (name inferred from function symbol)
func NewValidationNodeFromSchemas(
	schemas []any,
	formatter ValidationErrorFormatter,
) (*ValidationNode, error) {
	compiled, err := BuildValidationSchemas(schemas)
	if err != nil {
		return nil, err
	}
	if formatter == nil {
		formatter = DefaultValidationErrorFormatter
	}
	return &ValidationNode{schemasByName: compiled, formatError: formatter}, nil
}

// BuildValidationSchemas compiles schema inputs into validation entries keyed by tool name.
func BuildValidationSchemas(schemas []any) (map[string]ToolValidationSchema, error) {
	out := make(map[string]ToolValidationSchema, len(schemas))
	for _, schema := range schemas {
		entry, err := compileValidationSchema(schema)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(entry.Name) == "" {
			return nil, fmt.Errorf("prebuilt: validation schema name must not be empty")
		}
		if entry.Validator == nil {
			return nil, fmt.Errorf("prebuilt: validation schema %q has nil validator", entry.Name)
		}
		out[entry.Name] = entry
	}
	return out, nil
}

// Validate validates calls in parallel and returns results in input order.
func (n *ValidationNode) Validate(ctx context.Context, calls []ToolCall) ([]ToolMessage, error) {
	if len(calls) == 0 {
		return nil, nil
	}

	results := make([]ToolMessage, len(calls))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := range calls {
		idx := i
		call := calls[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			schema, ok := n.schemasByName[call.Name]
			if !ok {
				results[idx] = ToolMessage{
					ToolCallID: call.ID,
					Name:       call.Name,
					Status:     "error",
					Content:    fmt.Sprintf("No validator registered for tool %q", call.Name),
				}
				return
			}

			if err := schema.Validator(cloneArgs(call.Args)); err != nil {
				results[idx] = ToolMessage{
					ToolCallID: call.ID,
					Name:       call.Name,
					Status:     "error",
					Content:    n.formatError(err, call, schema),
				}
				return
			}

			b, err := json.Marshal(call.Args)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			results[idx] = ToolMessage{
				ToolCallID: call.ID,
				Name:       call.Name,
				Status:     "ok",
				Content:    string(b),
			}
		}()
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	return results, nil
}

func compileValidationSchema(source any) (ToolValidationSchema, error) {
	if source == nil {
		return ToolValidationSchema{}, fmt.Errorf("prebuilt: validation schema source must not be nil")
	}

	switch s := source.(type) {
	case ToolValidationSchema:
		if strings.TrimSpace(s.Name) == "" {
			return ToolValidationSchema{}, fmt.Errorf("prebuilt: validation schema name must not be empty")
		}
		return s, nil
	case *ToolValidationSchema:
		if s == nil {
			return ToolValidationSchema{}, fmt.Errorf("prebuilt: validation schema source must not be nil")
		}
		if strings.TrimSpace(s.Name) == "" {
			return ToolValidationSchema{}, fmt.Errorf("prebuilt: validation schema name must not be empty")
		}
		return *s, nil
	case ValidationSchemaSpec:
		return compileSchemaSpec(s)
	case *ValidationSchemaSpec:
		if s == nil {
			return ToolValidationSchema{}, fmt.Errorf("prebuilt: validation schema source must not be nil")
		}
		return compileSchemaSpec(*s)
	case ToolArgsSchemaProvider:
		return compileSchemaSpec(ValidationSchemaSpec{Name: s.Name(), Schema: s.ArgsSchema()})
	}

	return compileSchemaSpec(ValidationSchemaSpec{Name: inferSchemaName(source), Schema: source})
}

func compileSchemaSpec(spec ValidationSchemaSpec) (ToolValidationSchema, error) {
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return ToolValidationSchema{}, fmt.Errorf("prebuilt: validation schema name must not be empty")
	}
	validator, err := compileToolArgsValidator(spec.Schema)
	if err != nil {
		return ToolValidationSchema{}, fmt.Errorf("prebuilt: validation schema %q: %w", name, err)
	}
	return ToolValidationSchema{Name: name, Source: spec.Schema, Validator: validator}, nil
}

func compileToolArgsValidator(source any) (ToolArgsValidator, error) {
	if source == nil {
		return nil, fmt.Errorf("schema source must not be nil")
	}

	if validator, ok := source.(ToolArgsValidator); ok {
		return validator, nil
	}

	t := reflect.TypeOf(source)
	validatorType := reflect.TypeOf((*ToolArgsValidator)(nil)).Elem()
	if t.Kind() == reflect.Func && t.AssignableTo(validatorType) {
		converted := reflect.ValueOf(source).Convert(validatorType)
		validator, _ := converted.Interface().(ToolArgsValidator)
		return validator, nil
	}
	if t.Kind() == reflect.Func {
		return compileFunctionSchemaValidator(t)
	}
	if t == reflect.TypeOf((*reflect.Type)(nil)).Elem() {
		return nil, fmt.Errorf("unsupported schema source %T", source)
	}

	schemaType := t
	if rt, ok := source.(reflect.Type); ok {
		schemaType = rt
	}
	if schemaType == nil {
		return nil, fmt.Errorf("schema type must not be nil")
	}
	if schemaType.Kind() == reflect.Pointer {
		schemaType = schemaType.Elem()
	}
	if schemaType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported schema source %T", source)
	}
	return structSchemaValidator(schemaType), nil
}

func compileFunctionSchemaValidator(t reflect.Type) (ToolArgsValidator, error) {
	if t.NumIn() != 1 {
		return nil, fmt.Errorf("callable schema must accept exactly one argument, got %d", t.NumIn())
	}
	argType := t.In(0)
	if argType.Kind() == reflect.Pointer {
		argType = argType.Elem()
	}
	if argType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("callable schema argument must be struct-compatible, got %s", t.In(0))
	}
	return structSchemaValidator(argType), nil
}

func structSchemaValidator(schemaType reflect.Type) ToolArgsValidator {
	return func(args map[string]any) error {
		ptr := reflect.New(schemaType)
		decoder := json.NewDecoder(bytes.NewReader(mustJSON(args)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(ptr.Interface()); err != nil {
			return buildSchemaValidationError(schemaType, err)
		}
		if err := runSchemaModelValidation(ptr.Interface()); err != nil {
			return buildSchemaValidationError(schemaType, err)
		}
		if err := runSchemaModelValidation(ptr.Elem().Interface()); err != nil {
			return buildSchemaValidationError(schemaType, err)
		}
		return nil
	}
}

func runSchemaModelValidation(candidate any) error {
	model, ok := candidate.(ToolArgsSchemaModel)
	if !ok {
		return nil
	}
	return model.Validate()
}

func buildSchemaValidationError(schemaType reflect.Type, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(ToolValidationError); ok {
		return err
	}
	issues := []ToolArgValidationIssue{{Message: err.Error()}}
	if typeErr, ok := err.(*json.UnmarshalTypeError); ok {
		issues[0].Field = strings.TrimSpace(typeErr.Field)
	}
	if unknown := parseUnknownField(err); unknown != "" {
		issues[0].Field = unknown
		issues[0].Message = fmt.Sprintf("unknown field %q", unknown)
	}
	return schemaValidationError{schemaName: schemaType.Name(), issues: issues}
}

func parseUnknownField(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	const prefix = "json: unknown field "
	if !strings.HasPrefix(msg, prefix) {
		return ""
	}
	field := strings.TrimPrefix(msg, prefix)
	return strings.Trim(field, `"`)
}

func inferSchemaName(source any) string {
	if source == nil {
		return ""
	}
	if rt, ok := source.(reflect.Type); ok {
		return typeSchemaName(rt)
	}
	t := reflect.TypeOf(source)
	if t.Kind() == reflect.Func {
		pc := reflect.ValueOf(source).Pointer()
		if fn := runtime.FuncForPC(pc); fn != nil {
			parts := strings.Split(fn.Name(), ".")
			name := parts[len(parts)-1]
			name = strings.TrimSuffix(name, "-fm")
			return strings.TrimSpace(name)
		}
	}
	return typeSchemaName(t)
}

func typeSchemaName(t reflect.Type) string {
	if t == nil {
		return ""
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() != "" {
		return t.Name()
	}
	return t.String()
}

func mustJSON(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return b
}

type schemaValidationError struct {
	schemaName string
	issues     []ToolArgValidationIssue
}

func (e schemaValidationError) Error() string {
	if len(e.issues) == 0 {
		if strings.TrimSpace(e.schemaName) == "" {
			return "validation failed"
		}
		return fmt.Sprintf("validation failed for schema %q", e.schemaName)
	}
	parts := make([]string, 0, len(e.issues))
	for _, issue := range e.issues {
		field := strings.TrimSpace(issue.Field)
		if field == "" {
			parts = append(parts, issue.Message)
			continue
		}
		parts = append(parts, field+": "+issue.Message)
	}
	return strings.Join(parts, "; ")
}

func (e schemaValidationError) ValidationIssues() []ToolArgValidationIssue {
	out := make([]ToolArgValidationIssue, len(e.issues))
	copy(out, e.issues)
	return out
}
