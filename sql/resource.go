package sql

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"io"
	"path"
	"strings"
)

type resource struct {
	URL     string
	fs      afs.Service
	ctx     context.Context
	Reader  io.ReadCloser
	Origin  io.ReadCloser
	scanner *bufio.Scanner
	format  string
	line    []byte
}

type (
	resourceOptions struct {
		fs        afs.Service
		ctx       context.Context
		URL       string
		extension []string
		args      []driver.NamedValue
	}

	option func(r *resourceOptions)

	resources []*resource
)

func (r *resource) Close() {
	if r.Reader != nil {
		_ = r.Reader.Close()
		r.Reader = nil
	}
	if r.Origin != nil {
		_ = r.Origin.Close()
		r.Origin = nil
	}
}

func (r *resource) Next() (bool, error) {
	var err error
	_, name := url.Split(r.URL, file.Scheme)
	isCompressed := strings.HasSuffix(name, ".gz")

	if isCompressed {
		name = name[:len(name)-3]
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".json":
		r.format = "json"
	case ".yaml":
		r.format = "json"
	default:
		return false, fmt.Errorf("unsupported format: %v", r.format)
	}

	if r.Reader == nil {
		if r.Reader, err = r.fs.OpenURL(r.ctx, r.URL); err != nil {
			return false, err
		}
		if isCompressed {
			r.Origin = r.Reader
			if r.Reader, err = gzip.NewReader(r.Origin); err != nil {
				return false, err
			}
		}
	}

	if r.scanner == nil {
		r.scanner = bufio.NewScanner(r.Reader)
		r.scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)
	}
	if !r.scanner.Scan() {
		return false, nil
	}
	r.line = r.scanner.Bytes()
	return true, r.scanner.Err()
}

func (o *resourceOptions) locateResource(ctx context.Context) string {
	if ok, _ := o.fs.Exists(ctx, o.URL); ok {
		return o.URL
	}
	for _, ext := range o.extension {
		candidate := o.URL + ext
		if ok, _ := o.fs.Exists(ctx, candidate); ok {
			return candidate
		}
	}
	return ""
}

func withFS(fs afs.Service) option {
	return func(o *resourceOptions) {
		o.fs = fs
	}
}

func withURL(URL string) option {
	return func(o *resourceOptions) {
		o.URL = URL
	}
}

func withArgs(args []driver.NamedValue) option {
	return func(o *resourceOptions) {
		o.args = args
	}
}

func withExtension(extension ...string) option {
	return func(o *resourceOptions) {
		o.extension = extension
	}
}

func newResources(ctx context.Context, option ...option) (resources, error) {
	var ret = resources{}
	opts := &resourceOptions{}
	apply(option, opts)
	sourceURL := opts.locateResource(ctx)
	if sourceURL == "" {
		return ret, fmt.Errorf("invalid source: %v", opts.URL)
	}
	object, err := opts.fs.Object(ctx, sourceURL)
	if err != nil {
		return ret, err
	}
	if object.IsDir() {
		return ret, fmt.Errorf("unsupported directory: %v", opts.URL)
	}
	ret = append(ret, &resource{
		URL: sourceURL,
		fs:  opts.fs,
	})
	return ret, nil
}

func apply(option []option, opts *resourceOptions) {
	for _, o := range option {
		o(opts)
	}
	if opts.fs == nil {
		opts.fs = afs.New()
	}
	if len(opts.extension) == 0 {
		opts.extension = []string{".json", ".json.gz", ".yaml", ".yaml.gz"}
	}
}
