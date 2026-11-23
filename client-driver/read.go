package clientdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

func (d *driver) Read(ctx context.Context, key string) (string, error) {
	addr := d.leaderAddress
	if d.consistency == anyNodeRead {
		addr = d.next()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/values/%s", addr, key), http.NoBody)
	if err != nil {
		return "", err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}
	d.consistency = resp.Header.Get("read-consistency")

	var val string
	if err := json.NewDecoder(resp.Body).Decode(&val); err != nil {
		return "", err
	}
	return val, err
}

func (d *driver) next() string {
	i := d.index.Add(1) - 1
	return d.nodeAddresses[i%uint64(len(d.nodeAddresses))]
}
