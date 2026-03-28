package graph

import (
	"reflect"
	"testing"
)

func TestGenerateJSONSchema_StructTagsAndDoc(t *testing.T) {
	type schemaInput struct {
		Visible string   `json:"visible" doc:"visible field"`
		Skip    string   `json:"-"`
		Items   []string `json:"items"`
		Count   int      `json:"count"`
	}

	schema := generateJSONSchema(reflect.TypeOf(schemaInput{}), "SchemaInput")
	if schema.Type != "object" {
		t.Fatalf("schema.Type = %q, want object", schema.Type)
	}
	if schema.Properties["visible"] == nil || schema.Properties["visible"].Type != "string" {
		t.Fatalf("visible schema = %#v", schema.Properties["visible"])
	}
	if schema.Properties["visible"].Description != "visible field" {
		t.Fatalf("visible description = %q, want visible field", schema.Properties["visible"].Description)
	}
	if schema.Properties["count"] == nil || schema.Properties["count"].Type != "integer" {
		t.Fatalf("count schema = %#v", schema.Properties["count"])
	}
	if schema.Properties["items"] == nil || schema.Properties["items"].Type != "array" {
		t.Fatalf("items schema = %#v", schema.Properties["items"])
	}
	if schema.Properties["-"] != nil || schema.Properties["Skip"] != nil || schema.Properties["hidden"] != nil {
		t.Fatalf("unexpected skipped fields present: %#v", schema.Properties)
	}
}

func TestGenerateJSONSchemaWithChannels_DelegatesForNonStruct(t *testing.T) {
	plain := generateJSONSchema(reflect.TypeOf(""), "Text")
	withChannels := generateJSONSchemaWithChannels(reflect.TypeOf(""), map[string]Channel{"x": NewLastValue()}, "Text")
	if plain.Type != withChannels.Type || plain.Title != withChannels.Title {
		t.Fatalf("with channels schema = %#v, want %#v", withChannels, plain)
	}
}
