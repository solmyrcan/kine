package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rancher/kine/pkg/endpoint"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// newKine spins up a new instance of kine. it also registers cleanup functions for temporary data
//
// newKine is currently hardcoded to using sqlite and a unix socket listener, but might be extended in the future
//
// newKine will panic in case of error
//
// newKine will return a context as well as a configured etcd client for the kine instance
func newKine(tb testing.TB) *clientv3.Client {
	logrus.SetLevel(logrus.ErrorLevel)

	dir, err := os.MkdirTemp("testdata", "dir-*")
	if err != nil {
		panic(err)
	}
	tb.Cleanup(func() {
		os.RemoveAll(dir)
	})
	listener := fmt.Sprintf("unix://%s/listen.sock", dir)
	ep := fmt.Sprintf("sqlite://%s/data.db", dir)
	config, err := endpoint.Listen(context.Background(), endpoint.Config{
		Listener: listener,
		Endpoint: ep,
	})
	if err != nil {
		panic(err)
	}
	tlsConfig, err := config.TLSConfig.ClientConfig()
	if err != nil {
		panic(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{listener},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	})
	if err != nil {
		panic(err)
	}
	return client
}
Footer
© 2023 GitHub, Inc.
Footer navigation

    Terms
    Privacy
    Security
    Status
    Docs
    Contact GitHub
    Pricing
    API
    Training
    Blog
    About

unc (d *Generic) GetCompactRevision(ctx context.Context) (int64, error) {
	id, err := d.queryInt64(ctx, compactRevSQL)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}
