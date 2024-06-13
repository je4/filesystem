package config

import "embed"

//go:embed remotefs.toml
var ConfigFS embed.FS
