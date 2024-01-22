package autocert

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/caddyserver/certmagic"

	"github.com/pomerium/pomerium/pkg/zero/cluster"
)

type zeroStorage struct {
	client cluster.ClientWithResponsesInterface
	locker *locker
}

func newZeroStorage(client cluster.ClientWithResponsesInterface) *zeroStorage {
	s := &zeroStorage{
		client: client,
	}
	s.locker = &locker{
		store:  s.Store,
		load:   s.Load,
		delete: s.Delete,
	}
	return s
}

func (s *zeroStorage) Store(ctx context.Context, key string, value []byte) error {
	_, err := s.client.AutocertStoreWithBodyWithResponse(ctx, key, "application/octet-stream", bytes.NewReader(value))
	return err
}

func (s *zeroStorage) Load(ctx context.Context, key string) ([]byte, error) {
	res, err := s.client.AutocertLoadWithResponse(ctx, key)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (s *zeroStorage) Delete(ctx context.Context, key string) error {
	_, err := s.client.AutocertDeleteWithResponse(ctx, key)
	return err
}

func (s *zeroStorage) Exists(ctx context.Context, key string) bool {
	_, err := s.client.AutocertStatWithResponse(ctx, key)
	return err == nil
}

func (s *zeroStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	res, err := s.client.AutocertListWithResponse(ctx, &cluster.AutocertListParams{
		Prefix:    &prefix,
		Recursive: &recursive,
	})
	if err != nil {
		return nil, err
	}

	if res.JSON200 == nil {
		return nil, fmt.Errorf("unexpected response: %d %s", res.StatusCode(), res.Status())
	}

	return *res.JSON200, nil
}

func (s *zeroStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	res, err := s.client.AutocertStatWithResponse(ctx, key)
	if err != nil {
		return certmagic.KeyInfo{}, err
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
