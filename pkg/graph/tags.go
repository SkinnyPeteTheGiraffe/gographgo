package graph

func nodeTags[State any](node *NodeSpec[State]) []string {
	if node == nil || len(node.Metadata) == 0 {
		return nil
	}
	raw, ok := node.Metadata["tags"]
	if !ok || raw == nil {
		return nil
	}
	return coerceTags(raw)
}

func taskHasTag[State any](task pregelTask[State], tag string) bool {
	for _, t := range task.tags {
		if t == tag {
			return true
		}
	}
	return false
}

func nodeSpecHasTag[State any](node *NodeSpec[State], tag string) bool {
	if node == nil || len(node.Metadata) == 0 {
		return false
	}
	raw, ok := node.Metadata["tags"]
	if !ok || raw == nil {
		return false
	}
	for _, t := range coerceTags(raw) {
		if t == tag {
			return true
		}
	}
	return false
}

func coerceTags(raw any) []string {
	switch v := raw.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, tag := range v {
			if tag == "" {
				continue
			}
			out = append(out, tag)
		}
		return out
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			tag, ok := item.(string)
			if !ok || tag == "" {
				continue
			}
			out = append(out, tag)
		}
		return out
	default:
		return nil
	}
}

func hasAllInterrupt(items []string) bool {
	for _, item := range items {
		if item == All {
			return true
		}
	}
	return false
}
