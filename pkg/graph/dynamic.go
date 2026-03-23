package graph

// Dynamic is an explicit wrapper for runtime dynamic values.
//
// Use this instead of passing raw `any` across graph runtime boundaries.
type Dynamic struct {
	v any
}

// Dyn wraps a runtime value as Dynamic.
func Dyn(v any) Dynamic {
	return Dynamic{v: v}
}

// Value returns the wrapped runtime value.
func (d Dynamic) Value() any {
	return d.v
}

// DynMap wraps every value in map with Dyn.
func DynMap(values map[string]any) map[string]Dynamic {
	out := make(map[string]Dynamic, len(values))
	for k, v := range values {
		out[k] = Dyn(v)
	}
	return out
}
