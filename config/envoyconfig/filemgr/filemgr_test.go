package filemgr

import (
	"os"
	"path/filepath"
	"testing"

	envoy_config_core_v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	t.Run("bytes", func(t *testing.T) {
		mgr := NewManager(WithCacheDir(dir))
		ds := mgr.BytesDataSource("test.txt", []byte{1, 2, 3, 4, 5})
		assert.Equal(t, &envoy_config_core_v3.DataSource{
			Specifier: &envoy_config_core_v3.DataSource_Filename{
				Filename: filepath.Join(dir, "test-31443434314d425355414b4539.txt"),
			},
		}, ds)
		mgr.ClearCache()
	})

	t.Run("file", func(t *testing.T) {
		tmpFilePath := filepath.Join(dir, "test.txt")
		_ = os.WriteFile(tmpFilePath, []byte("TEST1"), 0o777)

		mgr := NewManager(WithCacheDir(dir))

		ds := mgr.FileDataSource(tmpFilePath)
		assert.Equal(t, &envoy_config_core_v3.DataSource{
			Specifier: &envoy_config_core_v3.DataSource_Filename{
				Filename: filepath.Join(dir, "test-3246454c394658475133414f35.txt"),
			},
		}, ds)

		_ = os.WriteFile(tmpFilePath, []byte("TEST2"), 0o777)

		ds = mgr.FileDataSource(tmpFilePath)
		assert.Equal(t, &envoy_config_core_v3.DataSource{
			Specifier: &envoy_config_core_v3.DataSource_Filename{
				Filename: filepath.Join(dir, "test-33343439385257475847375443.txt"),
			},
		}, ds)

		mgr.ClearCache()
	})
}

func Test_Source(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "test1.txt"), []byte("TEST-1"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test3.txt"), []byte("TEST-3"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test5.txt"), []byte("TEST-5"), 0o600))

	src := MultiSource("combined.txt", []byte{'|'},
		FileSource(filepath.Join(dir, "test1.txt")),
		BytesSource("test2.txt", []byte("TEST-2")),
		FileSource(filepath.Join(dir, "test3.txt")),
		BytesSource("test4.txt", []byte("TEST-4")),
		FileSource(filepath.Join(dir, "test5.txt")),
	)
	n, err := src.Checksum()
	assert.NoError(t, err)

	combinedFilePath := filepath.Join(dir, GetFileNameWithChecksum("combined.txt", n))

	mgr := NewManager(WithCacheDir(dir))
	ds, err := mgr.DataSource(src)
	assert.NoError(t, err)
	assert.Equal(t, &envoy_config_core_v3.DataSource{
		Specifier: &envoy_config_core_v3.DataSource_Filename{
			Filename: combinedFilePath,
		},
	}, ds)

	ds, err = mgr.DataSource(src)
	assert.NoError(t, err)
	assert.Equal(t, &envoy_config_core_v3.DataSource{
		Specifier: &envoy_config_core_v3.DataSource_Filename{
			Filename: combinedFilePath,
		},
	}, ds)

	bs, err := os.ReadFile(combinedFilePath)
	assert.NoError(t, err)
	assert.Equal(t, "TEST-1|TEST-2|TEST-3|TEST-4|TEST-5", string(bs))
}
