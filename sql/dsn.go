package sql

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"net/url"
	"os"
	"path"
	"strings"
)

// Config represent Connection config
type Config struct {
	BaseURL string
	url.Values
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	URL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %v", err)
	}
	cfg := &Config{
		Values: URL.Query(),
	}

	if URL.Scheme == "file" && URL.Path != "" {
		fs := afs.New()
		cwd, _ := os.Getwd()
		candidate := path.Join(cwd, URL.Path[1:])
		if ok, _ := fs.Exists(context.Background(), candidate); ok {
			URL.Path = candidate
		}
	}

	cfg.BaseURL = URL.Scheme + "://" + URL.Host + URL.Path
	if idx := strings.Index(dsn, "?"); idx != -1 {
		cfg.BaseURL = dsn[:idx]
	}
	if len(cfg.Values) > 0 {
		var unsupported []string
		for k := range cfg.Values {
			unsupported = append(unsupported, k)
		}
		return nil, fmt.Errorf("unsupported options: %v", unsupported)
	}
	return cfg, nil
}
