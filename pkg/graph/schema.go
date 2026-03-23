package graph

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"
)

type JSONSchema struct {
	Type                 string                 `json:"type,omitempty"`
	Properties           map[string]*JSONSchema `json:"properties,omitempty"`
	Items                *JSONSchema            `json:"items,omitempty"`
	AdditionalProperties interface{}            `json:"additionalProperties,omitempty"`
	Ref                  string                 `json:"$ref,omitempty"`
	Title                string                 `json:"title,omitempty"`
	Description          string                 `json:"description,omitempty"`
	Default              interface{}            `json:"default,omitempty"`
	Format               string                 `json:"format,omitempty"`
	Enum                 []interface{}          `json:"enum,omitempty"`
}

func generateJSONSchema(t reflect.Type, name string) *JSONSchema {
	if t == nil {
		return &JSONSchema{Type: "object"}
	}

	switch t.Kind() {
	case reflect.Bool:
		return &JSONSchema{Type: "boolean", Title: name}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &JSONSchema{Type: "integer", Title: name}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &JSONSchema{Type: "integer", Title: name}
	case reflect.Float32, reflect.Float64:
		return &JSONSchema{Type: "number", Title: name}
	case reflect.String:
		return &JSONSchema{Type: "string", Title: name}
	case reflect.Slice, reflect.Array:
		elemSchema := generateJSONSchema(t.Elem(), t.Elem().Name())
		return &JSONSchema{
			Type:  "array",
			Title: name,
			Items: elemSchema,
		}
	case reflect.Map:
		return &JSONSchema{
			Type:                 "object",
			Title:                name,
			AdditionalProperties: true,
		}
	case reflect.Struct:
		properties := make(map[string]*JSONSchema)
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}
			fieldName := field.Name
			jsonTag := field.Tag.Get("json")
			if jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] != "" {
					fieldName = parts[0]
				}
				if parts[0] == "-" {
					continue
				}
			}
			fieldSchema := generateJSONSchema(field.Type, field.Name)
			if doc := field.Tag.Get("doc"); doc != "" {
				fieldSchema.Description = doc
			}
			properties[fieldName] = fieldSchema
		}
		return &JSONSchema{
			Type:       "object",
			Title:      name,
			Properties: properties,
		}
	case reflect.Ptr:
		return generateJSONSchema(t.Elem(), name)
	default:
		return &JSONSchema{Type: "object", Title: name}
	}
}

func GetInputJSONSchema(t any, name string) map[string]any {
	schema := generateJSONSchema(reflect.TypeOf(t), name)
	data, _ := json.Marshal(schema)
	result := make(map[string]any)
	json.Unmarshal(data, &result)
	return result
}

func GetOutputJSONSchema(t any, name string) map[string]any {
	schema := generateJSONSchema(reflect.TypeOf(t), name)
	data, _ := json.Marshal(schema)
	result := make(map[string]any)
	json.Unmarshal(data, &result)
	return result
}

type SchemaGenerator struct {
	schemas map[reflect.Type]*JSONSchema
}

func NewSchemaGenerator() *SchemaGenerator {
	return &SchemaGenerator{
		schemas: make(map[reflect.Type]*JSONSchema),
	}
}

func (sg *SchemaGenerator) Generate(t reflect.Type, name string) *JSONSchema {
	if schema, ok := sg.schemas[t]; ok {
		return schema
	}
	schema := generateJSONSchema(t, name)
	sg.schemas[t] = schema
	return schema
}

func (sg *SchemaGenerator) AddCustomType(t reflect.Type, schema *JSONSchema) {
	sg.schemas[t] = schema
}

type JSONSchemaGenerator struct{}

func NewJSONSchemaGenerator() *JSONSchemaGenerator {
	return &JSONSchemaGenerator{}
}

func (g *JSONSchemaGenerator) Generate(t any, name string) map[string]any {
	schema := generateJSONSchema(reflect.TypeOf(t), name)
	data, _ := json.Marshal(schema)
	result := make(map[string]any)
	json.Unmarshal(data, &result)
	return result
}

func (g *JSONSchemaGenerator) GenerateWithChannels(t any, channels map[string]Channel, name string) map[string]any {
	schema := generateJSONSchemaWithChannels(reflect.TypeOf(t), channels, name)
	data, _ := json.Marshal(schema)
	result := make(map[string]any)
	json.Unmarshal(data, &result)
	return result
}

func generateJSONSchemaWithChannels(t reflect.Type, channels map[string]Channel, name string) *JSONSchema {
	if t == nil {
		return &JSONSchema{Type: "object"}
	}

	if t.Kind() == reflect.Struct {
		properties := make(map[string]*JSONSchema)
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}
			fieldName := field.Name
			jsonTag := field.Tag.Get("json")
			if jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] != "" {
					fieldName = parts[0]
				}
				if parts[0] == "-" {
					continue
				}
			}

			fieldSchema := generateJSONSchema(field.Type, field.Name)
			if ch, ok := channels[fieldName]; ok {
				fieldSchema = mergeChannelInfo(fieldSchema, ch)
			}
			if doc := field.Tag.Get("doc"); doc != "" {
				fieldSchema.Description = doc
			}
			properties[fieldName] = fieldSchema
		}
		return &JSONSchema{
			Type:       "object",
			Title:      name,
			Properties: properties,
		}
	}

	return generateJSONSchema(t, name)
}

func mergeChannelInfo(schema *JSONSchema, ch Channel) *JSONSchema {
	return schema
}

func formatTimeRFC3339(t time.Time) string {
	return t.Format(time.RFC3339)
}
