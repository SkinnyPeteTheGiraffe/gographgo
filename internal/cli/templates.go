package cli

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Template describes a project bootstrap template.
type Template struct {
	Description string
	URL         string
}

var templates = map[string]Template{
	"deep-agent-python": {
		Description: "An opinionated deployment template for a Deep Agent.",
		URL:         "https://github.com/langchain-ai/deep-agent-template/archive/refs/heads/main.zip",
	},
	"deep-agent-js": {
		Description: "An opinionated deployment template for a Deep Agent.",
		URL:         "https://github.com/langchain-ai/deep-agent-template-js/archive/refs/heads/main.zip",
	},
	"agent-python": {
		Description: "A simple agent that can be flexibly extended to many tools.",
		URL:         "https://github.com/langchain-ai/simple-agent-template/archive/refs/heads/main.zip",
	},
	"new-langgraph-project-python": {
		Description: "A simple, minimal chatbot with memory.",
		URL:         "https://github.com/langchain-ai/new-langgraph-project/archive/refs/heads/main.zip",
	},
	"new-langgraph-project-js": {
		Description: "A simple, minimal chatbot with memory.",
		URL:         "https://github.com/langchain-ai/new-langgraphjs-project/archive/refs/heads/main.zip",
	},
}

var httpGet = func(url string) (*http.Response, error) {
	return http.Get(url)
}

// CreateNew creates a new project directory from a remote template.
func CreateNew(path, templateID string, stdout io.Writer) error {
	if strings.TrimSpace(path) == "" {
		path = "."
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if err := ensureEmptyDir(abs); err != nil {
		return err
	}

	t, err := resolveTemplate(templateID)
	if err != nil {
		return err
	}

	resp, err := httpGet(t.URL)
	if err != nil {
		return fmt.Errorf("download template: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download template: unexpected status %s", resp.Status)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read template archive: %w", err)
	}
	if err := unzipInto(bytes.NewReader(b), int64(len(b)), abs); err != nil {
		return err
	}
	if stdout != nil {
		_, _ = fmt.Fprintf(stdout, "new project created at %s\n", abs)
	}
	return nil
}

func resolveTemplate(templateID string) (Template, error) {
	id := strings.TrimSpace(templateID)
	if id == "" {
		id = "new-langgraph-project-python"
	}
	t, ok := templates[id]
	if !ok {
		return Template{}, fmt.Errorf("template %q not found (available: %s)", id, strings.Join(templateIDs(), ", "))
	}
	return t, nil
}

func templateIDs() []string {
	out := make([]string, 0, len(templates))
	for id := range templates {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func ensureEmptyDir(path string) error {
	st, err := os.Stat(path)
	if err == nil {
		if !st.IsDir() {
			return fmt.Errorf("path exists and is not a directory: %s", path)
		}
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return readErr
		}
		if len(entries) > 0 {
			return fmt.Errorf("directory is not empty: %s", path)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(path, 0o755)
}

func unzipInto(r io.ReaderAt, size int64, dest string) error {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	rootPrefix := detectArchiveRoot(zr)
	for _, f := range zr.File {
		rel := strings.TrimPrefix(f.Name, rootPrefix)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" {
			continue
		}
		target := filepath.Join(dest, filepath.FromSlash(rel))
		if !strings.HasPrefix(target, dest+string(os.PathSeparator)) && target != dest {
			return fmt.Errorf("invalid archive entry: %s", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		func() {
			defer func() { _ = rc.Close() }()
			out, createErr := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
			if createErr != nil {
				err = createErr
				return
			}
			defer func() { _ = out.Close() }()
			_, err = io.Copy(out, rc)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func detectArchiveRoot(zr *zip.Reader) string {
	if len(zr.File) == 0 {
		return ""
	}
	var prefix string
	for _, f := range zr.File {
		name := strings.TrimPrefix(f.Name, "/")
		parts := strings.SplitN(name, "/", 2)
		if len(parts) < 2 || parts[0] == "" {
			return ""
		}
		if prefix == "" {
			prefix = parts[0]
			continue
		}
		if prefix != parts[0] {
			return ""
		}
	}
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}
