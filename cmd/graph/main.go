package main

import (
	"context"
	"os"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/cli"
)

func main() {
	code := cli.Main(context.Background(), os.Args[1:], os.Stdout, os.Stderr)
	os.Exit(code)
}
