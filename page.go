package gitlfsfuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
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
	Header        map[string]string
	Href          string
	Authenticated bool
}

type PageFetcher interface {
	Fetch(ctx context.Context, w io.Writer, ptr *lfs.Pointer, off, end int64, pageNum string) error
}

type pageFetcher struct {
	manifest  tq.Manifest
	lru       DoubleLRU
	actions   *otter.CacheWithVariableTTL[string, action]
	remoteRef *git.Ref
	pl        *plock
	remote    string
	pr        string
	maxPages  int64
	mu        sync.Mutex
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
		return action{}, &tq.ObjectError{Code: http.StatusNotFound, Message: "Object does not exist"}
	}
	transfer := *br.Objects[0]
	if transfer.Error != nil {
		return action{}, transfer.Error
	}
	rel := transfer.Actions["download"]
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
	if resp != nil && resp.Body != nil {
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := resp.Header.Get("Retry-After")
		if seconds, err := strconv.Atoi(retryAfter); err == nil {
			return off, &retryErr{after: time.Duration(seconds) * time.Second}
		}
		if date, err := time.Parse(time.RFC1123, retryAfter); err == nil {
			return off, &retryErr{after: time.Until(date)}
		}
		return off, &retryErr{after: time.Second} // TODO: backoff + random jitter
	}
	if resp != nil && resp.StatusCode != http.StatusPartialContent {
		return off, errors.New("unexpected status code: " + resp.Status)
	}
	if err != nil {
		return off, err
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
		oid, pn := p.lru.First()
		path := filepath.Join(p.pr, oid, "shared", pn)
		if !p.pl.TryLock(path) {
			break
		}
		err := os.Remove(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			p.pl.Unlock(path)
			p.mu.Unlock()
			return err
		}
		err = p.lru.Delete(oid, pn)
		if err != nil {
			p.pl.Unlock(path)
			p.mu.Unlock()
			return err
		}
		p.pl.Unlock(path)
	}
	p.mu.Unlock()

	a, err := p.getAction(ctx, ptr)
	if err != nil {
		return err
	}
	var r *retryErr
	for nextOff := off; nextOff < end && err == nil; {
		nextOff, err = p.download(ctx, w, a, nextOff, end)
		log.Printf("fetch: oid=(%s) %dMB %s/%d err=%v", ptr.Oid, pagesize/(1024*1024), pageNum, int64(math.Ceil(float64(ptr.Size/pagesize)))-1, err)
		if errors.As(err, &r) {
			err = nil
			time.Sleep(r.after)
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

func (e *retryErr) Error() string { return "HTTP 429: retry after " + e.after.String() }
