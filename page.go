package gitlfsfuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/git-lfs/git-lfs/v3/git"
	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/git-lfs/git-lfs/v3/tq"
	"github.com/maypok86/otter"
)

type action struct {
	Href          string
	Header        map[string]string
	Authenticated bool
}

type PageFetcher interface {
	Fetch(ctx context.Context, w io.Writer, ptr *lfs.Pointer, off, end int64, pageNum string) error
}

type pageFetcher struct {
	remote    string
	actions   *otter.CacheWithVariableTTL[string, action]
	remoteRef *git.Ref
	manifest  tq.Manifest
	mu        sync.Mutex
	cacheDir  string
	lru       DoubleLRU
	maxPages  int64
	pr 		  string
}

func (p *pageFetcher) getAction(ctx context.Context, ptr *lfs.Pointer) (action, error) {
	a, ok := p.actions.Get(ptr.Oid)
	if ok {
		return a, nil
	}
	// TODO: single flight, aggregate concurrent getActions and respect the ctx.
	br, err := tq.Batch(p.manifest, tq.Download, p.remote, p.remoteRef, []*tq.Transfer{{Oid: ptr.Oid, Size: ptr.Size}})
	if err != nil {
		return action{}, err
	}
	if len(br.Objects) == 0 {
		return action{}, errors.New("no objects found")
	}
	transfer := *br.Objects[0]
	if transfer.Error != nil {
		return action{}, transfer.Error
	}
	rel := transfer.Actions["download"]
	if rel == nil {
		rel = transfer.Links["download"]
	}
	if rel == nil {
		return action{}, errors.New("no download action found")
	}
	a = action{
		Href:          rel.Href,
		Header:        rel.Header,
		Authenticated: transfer.Authenticated,
	}
	if rel.ExpiresIn > 0 {
		p.actions.Set(ptr.Oid, a, time.Duration(rel.ExpiresIn)*time.Second)
	} else if !rel.ExpiresAt.IsZero() {
		p.actions.Set(ptr.Oid, a, rel.ExpiresAt.Sub(time.Now()))
	} else {
		p.actions.Set(ptr.Oid, a, time.Minute*5)
	}
	return a, nil
}

func (p *pageFetcher) download(ctx context.Context, w io.Writer, a action, off, end int64) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", a.Href, nil)
	if err != nil {
		return off, err
	}
	for header, value := range a.Header {
		req.Header.Add(header, value)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end-1))

	var resp *http.Response
	if a.Authenticated {
		resp, err = p.manifest.APIClient().Do(req)
	} else {
		resp, err = p.manifest.APIClient().DoAPIRequestWithAuth(p.remote, req)
	}
	if err != nil {
		return off, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := resp.Header.Get("Retry-After")
		if seconds, err := strconv.Atoi(retryAfter); err == nil {
			return off, &retryErr{after: time.Duration(seconds) * time.Second}
		}
		if date, err := time.Parse(time.RFC1123, retryAfter); err == nil {
			return off, &retryErr{after: time.Until(date)}
		}
		return off, &retryErr{after: time.Second} // TODO: backoff + random jitter
	}
	if resp.StatusCode != http.StatusPartialContent {
		return off, errors.New("unexpected status code: " + resp.Status)
	}
	rangeHdr := resp.Header.Get("Content-Range")
	regex := regexp.MustCompile(`bytes (\d+)\-.*`)
	match := regex.FindStringSubmatch(rangeHdr)
	if len(match) < 2 {
		return off, fmt.Errorf("badly formatted Content-Range header: %q", rangeHdr)
	}
	if contentStart, _ := strconv.ParseInt(match[1], 10, 64); contentStart != off {
		return off, fmt.Errorf("Content-Range start byte incorrect: %s expected %d", match[1], off)
	}
	n, err := io.Copy(w, resp.Body)
	return off + n, err
}

func (p *pageFetcher) Fetch(ctx context.Context, w io.Writer, ptr *lfs.Pointer, off, end int64, pageNum string) error {
	// TODO: single flight by ptr.Oid+range and make it asyncable.
	p.mu.Lock()

	for p.lru.Size() >= p.maxPages {
		oid, pageNum := p.lru.First()
		path := filepath.Join(p.pr, oid, "shared", pageNum)
		err := os.Remove(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			p.mu.Unlock()
			return err
		}
		err = p.lru.Delete(oid, pageNum)
		if err != nil {
			p.mu.Unlock()
			return err
		}
	}
	p.mu.Unlock()

	a, err := p.getAction(ctx, ptr)
	if err != nil {
		return err
	}
	for nextOff := off; nextOff < end && err == nil; {
		nextOff, err = p.download(ctx, w, a, nextOff, end)
		log.Printf("fetch: oid=(%s) size=(%s) %6.2f%% err=%v", ptr.Oid, humanReadableSize(ptr.Size), float64(nextOff)*100/float64(ptr.Size), err)
		if err != nil {
			if r, ok := err.(*retryErr); ok {
				err = nil
				time.Sleep(r.after)
			}
		}
	}
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	err = p.lru.Add(ptr.Oid, pageNum)
	if err != nil {
		return err
	}
	return err
}

type retryErr struct {
	after time.Duration
}

func (e *retryErr) Error() string { return "retry after " + e.after.String() }

func humanReadableSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
