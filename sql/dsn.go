package sql

import (
	"fmt"
	"net/url"
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
	if URL.Scheme != scheme {
		return nil, fmt.Errorf("invalid dsn scheme, expected %v, but had: %v", scheme, URL.Scheme)
	}
	cfg := &Config{
		Values: URL.Query(),
	}
	host := URL.Host
	cfg.BaseURL = host + URL.Path
	cfg.BaseURL = strings.Replace(cfg.BaseURL, "$", "://", 1)
	if len(cfg.Values) > 0 {
		var unsupported []string
		for k := range cfg.Values {
			unsupported = append(unsupported, k)
		}
		return nil, fmt.Errorf("unsupported options: %v", unsupported)
	}
	return cfg, nil
}
