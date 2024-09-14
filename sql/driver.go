package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	_ "github.com/viant/afs/mem"
	"github.com/viant/x"
	"os"
	"path"
	"strings"
)

const (
	scheme = "structql"
)

func init() {
	sql.Register(scheme, &Driver{})
	fs := afs.New()
	_ = fs.Upload(context.Background(), "mem://localhost/structql/single.json", file.DefaultFileOsMode, strings.NewReader(`{}`))
}

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type Driver struct{}

// Open new Connection.
// See https://github.com/viant/structql#dsn-data-source-name for how
// the DSN string is formatted
func (d Driver) Open(dsn string) (driver.Conn, error) {
	if dsn == "" {
		return nil, fmt.Errorf("structql dsn was empty")
	}
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	ret := &Connection{
		cfg:   cfg,
		fs:    afs.New(),
		types: x.NewRegistry(),
	}
	if strings.HasPrefix(cfg.BaseURL, "/") {
		wd, _ := os.Getwd()
		URL := path.Join(wd, cfg.BaseURL[1:])
		if ok, _ := ret.fs.Exists(context.Background(), URL); ok {
			cfg.BaseURL = cfg.BaseURL[1:]
		}
	} else if ok, err := ret.fs.Exists(context.Background(), cfg.BaseURL); !ok {
		return ret, err
	}
	return ret, nil
}
