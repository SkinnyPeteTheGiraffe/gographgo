package cli

import (
	"archive/zip"
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateNew(t *testing.T) {
	t.Parallel()
	origGet := httpGet
	t.Cleanup(func() { httpGet = origGet })

	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)
	fw, err := zw.Create("repo-main/README.md")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := fw.Write([]byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	httpGet = func(_ string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(buf.Bytes())),
		}, nil
	}

	dir := t.TempDir()
	if err := CreateNew(dir, "new-langgraph-project-python", io.Discard); err != nil {
		t.Fatalf("CreateNew() error = %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(b) != "hello" {
		t.Fatalf("unexpected file contents = %q", string(b))
	}
}

func TestCreateNewInvalidTemplate(t *testing.T) {
	t.Parallel()
	err := CreateNew(t.TempDir(), "missing-template", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected template error, got %v", err)
	}
}
