package gitlfsfuse

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestPageFetcher_Error_503(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusServiceUnavailable)
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestPageFetcher_Error_404_1(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestPageFetcher_Error_404_2(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
			  "operation": "download",
			  "transfers": [ "basic" ],
			  "ref": { "name": "refs/heads/main" },
			  "objects": [],
			  "hash_algo": "sha256"
			}`))
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestPageFetcher_Error_404_3(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"operation": "download",
				"transfers": [ "basic" ],
				"ref": { "name": "refs/heads/main" },
				"objects": [
					{
						"error": {
							"code": 404,
							"message": "Object does not exist"
						}
					}
				],
				"hash_algo": "sha256"
			}`))
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestPageFetcher_Error_Missing_Link(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"operation": "download",
				"transfers": [ "basic" ],
				"ref": { "name": "refs/heads/main" },
				"objects": [
					{}
				],
				"hash_algo": "sha256"
			}`))
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestPageFetcher_FetchRetries(t *testing.T) {
	var retries int64
	hid, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/objects") {
				switch atomic.AddInt64(&retries, 1) {
				case 1:
					w.WriteHeader(http.StatusTooManyRequests)
					return
				case 2:
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					return
				case 3:
					w.Header().Set("Retry-After", time.Now().Add(time.Second).Format(time.RFC1123))
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
			}
			handler.ServeHTTP(w, r)
		})
	})
	defer cancel()

	_ = verifyRemoteFile(t, hid, mnt, "emptylarge.bin")
}

func TestPageFetcher_Fetch_WrongStatus(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/objects") {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			handler.ServeHTTP(w, r)
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestPageFetcher_Fetch_WrongRange(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/objects") {
				w.Header().Set("Content-Range", "bytes 0000000")
				w.WriteHeader(http.StatusPartialContent)
				return
			}
			handler.ServeHTTP(w, r)
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestPageFetcher_Fetch_WrongRange_2(t *testing.T) {
	_, mnt, cancel := cloneMountWithCacheSize(t, 100, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("%s %s", r.Method, r.URL)
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/objects") {
				w.Header().Set("Content-Range", "bytes 1111111-222222/2309409234")
				w.WriteHeader(http.StatusPartialContent)
				return
			}
			handler.ServeHTTP(w, r)
		})
	})
	defer cancel()

	if _, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin")); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}
