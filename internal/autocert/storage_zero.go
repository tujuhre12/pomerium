package autocert

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"

	"github.com/caddyserver/certmagic"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"

	"github.com/pomerium/pomerium/internal/telemetry"
	"github.com/pomerium/pomerium/pkg/zero/cluster"
)

type zeroStorage struct {
	client cluster.ClientWithResponsesInterface
	*locker
	telemetry telemetry.Component
}

func newZeroStorage(client cluster.ClientWithResponsesInterface) *zeroStorage {
	s := &zeroStorage{
		client:    client,
		telemetry: *telemetry.NewComponent(zerolog.InfoLevel, "autocert", "zero"),
	}
	s.locker = &locker{
		store:  s.Store,
		load:   s.Load,
		delete: s.Delete,
	}
	return s
}

func (s *zeroStorage) Store(ctx context.Context, key string, value []byte) error {
	ctx, op := s.telemetry.Start(ctx, "Store", attribute.String("key", key))
	defer op.Complete()

	res, err := s.client.AutocertStoreWithBodyWithResponse(ctx, key, "application/octet-stream", bytes.NewReader(value))
	if err != nil {
		return op.Failure(err)
	}
	if res.StatusCode()/100 != 2 {
		return op.Failure(fmt.Errorf("error storing key: %d %s", res.StatusCode(), res.Status()))
	}

	return nil
}

func (s *zeroStorage) Load(ctx context.Context, key string) ([]byte, error) {
	ctx, op := s.telemetry.Start(ctx, "Load", attribute.String("key", key))
	defer op.Complete()

	res, err := s.client.AutocertLoadWithResponse(ctx, key)
	if err != nil {
		return nil, op.Failure(err)
	}
	if res.StatusCode() == http.StatusNotFound {
		return nil, fs.ErrNotExist
	} else if res.StatusCode()/100 != 2 {
		return nil, op.Failure(fmt.Errorf("error loading key: %d %s", res.StatusCode(), res.Status()))
	}

	return res.Body, nil
}

func (s *zeroStorage) Delete(ctx context.Context, key string) error {
	ctx, op := s.telemetry.Start(ctx, "Delete", attribute.String("key", key))
	defer op.Complete()

	res, err := s.client.AutocertDeleteWithResponse(ctx, key)
	if err != nil {
		return op.Failure(err)
	}
	if res.StatusCode()/100 != 2 {
		return op.Failure(fmt.Errorf("error deleting key: %d %s", res.StatusCode(), res.Status()))
	}

	return nil
}

func (s *zeroStorage) Exists(ctx context.Context, key string) bool {
	ctx, op := s.telemetry.Start(ctx, "Exists", attribute.String("key", key))
	defer op.Complete()

	res, err := s.client.AutocertStatWithResponse(ctx, key)
	if err != nil {
		_ = op.Failure(err)
		return false
	}
	if res.StatusCode() == http.StatusNotFound {
		return false
	} else if res.StatusCode()/100 != 2 {
		_ = op.Failure(fmt.Errorf("error checking if key exists: %d %s", res.StatusCode(), res.Status()))
		return false
	}

	return true
}

func (s *zeroStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	ctx, op := s.telemetry.Start(ctx, "List", attribute.String("prefix", prefix))
	defer op.Complete()

	res, err := s.client.AutocertListWithResponse(ctx, &cluster.AutocertListParams{
		Prefix:    &prefix,
		Recursive: &recursive,
	})
	if err != nil {
		return nil, op.Failure(err)
	}
	if res.StatusCode()/100 != 2 {
		return nil, op.Failure(fmt.Errorf("error listing keys: %d %s", res.StatusCode(), res.Status()))
	}

	if res.JSON200 == nil {
		return nil, op.Failure(fmt.Errorf("unexpected response: %d %s", res.StatusCode(), res.Status()))
	}

	return *res.JSON200, nil
}

func (s *zeroStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	ctx, op := s.telemetry.Start(ctx, "Stat", attribute.String("key", key))
	defer op.Complete()

	res, err := s.client.AutocertStatWithResponse(ctx, key)
	if err != nil {
		return certmagic.KeyInfo{}, op.Failure(err)
	}
	if res.StatusCode()/100 != 2 {
		return certmagic.KeyInfo{}, op.Failure(fmt.Errorf("error getting key info: %d %s", res.StatusCode(), res.Status()))
	}

	info := certmagic.KeyInfo{
		Key: key,
	}

	if tm, err := http.ParseTime(res.HTTPResponse.Header.Get("Last-Modified")); err == nil {
		info.Modified = tm
	}

	if sz, err := strconv.ParseInt(res.HTTPResponse.Header.Get("Content-Length"), 10, 64); err == nil {
		info.Size = sz
	}

	if res.HTTPResponse.Header.Get("X-Is-Terminal") == "true" {
		info.IsTerminal = true
	}

	return info, nil
}
