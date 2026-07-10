//go:build tools

package tools

import (
	_ "golang.org/x/perf/cmd/benchstat"
	_ "golang.org/x/tools/cmd/godoc"
	_ "github.com/golangci/golangci-lint/v2/cmd/golangci-lint"
	_ "golang.org/x/exp/cmd/apidiff"
	_ "golang.org/x/vuln/cmd/govulncheck"
)
