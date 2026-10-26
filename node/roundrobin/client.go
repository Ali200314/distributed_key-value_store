package roundrobin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type httpClient struct {
	nodes  []string
	client *http.Client
	next   atomic.Uint64
}

type entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewClient(nodes []string) (Client, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("at least one node URL is required")
	}

	cleaned := make([]string, len(nodes))
	for i, n := range nodes {
		u, err := url.Parse(n)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("invalid node URL: %q", n)
		}
		cleaned[i] = strings.TrimRight(n, "/")
	}

	return &httpClient{
		nodes:  cleaned,
		client: &http.Client{Timeout: 5 * time.Second},
	}, nil
}

// node returns the next node in round-robin order (safe for concurrent use).
func (c *httpClient) node() string {
	return c.nodes[(c.next.Add(1)-1)%uint64(len(c.nodes))]
}

// do sends one request to the next node and returns the response body.
func (c *httpClient) do(method, path string, body any) ([]byte, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, c.node()+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: HTTP %d", method, path, resp.StatusCode)
	}
	return data, nil
}

func (c *httpClient) Read(key string) (string, error) {
	data, err := c.do(http.MethodGet, "/kv?key="+url.QueryEscape(key), nil)
	return string(data), err
}

func (c *httpClient) Create(key, value string) error {
	_, err := c.do(http.MethodPost, "/kv", entry{key, value})
	return err
}

func (c *httpClient) Update(key, value string) error {
	_, err := c.do(http.MethodPut, "/kv", entry{key, value})
	return err
}

func (c *httpClient) Delete(key string) error {
	_, err := c.do(http.MethodDelete, "/kv?key="+url.QueryEscape(key), nil)
	return err
}

var _ Client = (*httpClient)(nil)
