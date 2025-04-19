package gitlfsfuse

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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
