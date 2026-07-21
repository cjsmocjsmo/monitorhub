package main

import _ "embed"

// monhubHTML is embedded at build time so web UI deployments require only one binary.
//go:embed monhub.html
var monhubHTML []byte
