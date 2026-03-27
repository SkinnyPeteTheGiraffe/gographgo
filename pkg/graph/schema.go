package graph

import (
	"encoding/json"
	"reflect"
	"strings"
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
	if schema := generatePrimitiveJSONSchema(t.Kind(), name); schema != nil {
		return schema
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		return generateArrayJSONSchema(t, name)
	case reflect.Map:
		return generateMapJSONSchema(name)
	case reflect.Struct:
		return generateStructJSONSchema(t, name)
	case reflect.Ptr:
		return generateJSONSchema(t.Elem(), name)
	default:
		return &JSONSchema{Type: "object", Title: name}
	}
}

func generatePrimitiveJSONSchema(kind reflect.Kind, name string) *JSONSchema {
	switch kind {
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
	default:
		return nil
	}

}

func generateArrayJSONSchema(t reflect.Type, name string) *JSONSchema {
	elemSchema := generateJSONSchema(t.Elem(), t.Elem().Name())
	return &JSONSchema{Type: "array", Title: name, Items: elemSchema}
}

func generateMapJSONSchema(name string) *JSONSchema {
	return &JSONSchema{Type: "object", Title: name, AdditionalProperties: true}
}

func generateStructJSONSchema(t reflect.Type, name string) *JSONSchema {
	properties := buildStructProperties(t, nil)
	return &JSONSchema{Type: "object", Title: name, Properties: properties}
}

func buildStructProperties(t reflect.Type, channels map[string]Channel) map[string]*JSONSchema {
	properties := make(map[string]*JSONSchema)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldName, include := schemaFieldName(field)
		if !include {
			continue
		}
		fieldSchema := generateJSONSchema(field.Type, field.Name)
		if channels != nil {
			if channel, ok := channels[fieldName]; ok {
				fieldSchema = mergeChannelInfo(fieldSchema, channel)
			}
		}
		if doc := field.Tag.Get("doc"); doc != "" {
			fieldSchema.Description = doc
		}
		properties[fieldName] = fieldSchema
	}
	return properties
}

func schemaFieldName(field reflect.StructField) (name string, include bool) {
	if !field.IsExported() {
		return "", false
	}
	fieldName := field.Name
	jsonTag := field.Tag.Get("json")
	if jsonTag == "" {
		return fieldName, true
	}
	parts := strings.Split(jsonTag, ",")
	if parts[0] == "-" {
		return "", false
	}
	if parts[0] != "" {
		fieldName = parts[0]
	}
	return fieldName, true
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
	if t.Kind() != reflect.Struct {
		return generateJSONSchema(t, name)
	}
	properties := buildStructProperties(t, channels)
	return &JSONSchema{Type: "object", Title: name, Properties: properties}
}

func mergeChannelInfo(schema *JSONSchema, _ Channel) *JSONSchema {
	return schema
}
