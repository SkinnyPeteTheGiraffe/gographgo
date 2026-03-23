package sdk

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func withQuery(basePath string, query map[string]any) (string, error) {
	values := url.Values{}
	for key, value := range query {
		if value == nil {
			continue
		}
		switch v := value.(type) {
		case string:
			if v != "" {
				values.Set(key, v)
			}
		case bool:
			values.Set(key, strconv.FormatBool(v))
		case int:
			values.Set(key, strconv.Itoa(v))
		case int64:
			values.Set(key, strconv.FormatInt(v, 10))
		case float64:
			values.Set(key, strconv.FormatFloat(v, 'f', -1, 64))
		case []string:
			if len(v) > 0 {
				values.Set(key, strings.Join(v, ","))
			}
		default:
			return "", fmt.Errorf("sdk: unsupported query type for %s", key)
		}
	}
	if len(values) == 0 {
		return basePath, nil
	}
	return basePath + "?" + values.Encode(), nil
}

func containsStatus(allowed []int, status int) bool {
	for _, s := range allowed {
		if s == status {
			return true
		}
	}
	return false
}

func closeErr(ch chan error, err error) {
	ch <- err
	close(ch)
}
