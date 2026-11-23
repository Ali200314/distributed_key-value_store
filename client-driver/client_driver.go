package clientdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
)

const (
	anyNodeRead    string = "ANY_NODE_READ"
	leaderOnlyRead string = "LEADER_READ"
)

type Driver interface {
	Create(context.Context, string, string) error
	Delete(context.Context, string) error
	Update(context.Context, string, string) error
	Read(context.Context, string) (string, error)
}

type driver struct {
	httpClient    *http.Client
	nodeAddresses []string
	index         atomic.Uint64
	leaderAddress string
	consistency   string
}

func New(addr string, httpClient *http.Client) (Driver, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/nodes", addr), http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	d := &driver{
		httpClient: httpClient,
	}

	if err := json.NewDecoder(resp.Body).Decode(&d.nodeAddresses); err != nil {
		return nil, err
	}

	req, err = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/leader", addr), http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err = d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	var leaderAddr string
	if err := json.NewDecoder(resp.Body).Decode(&leaderAddr); err != nil {
		return nil, err
	}
	d.leaderAddress = leaderAddr

	return d, nil
}

func (d *driver) Create(ctx context.Context, key, value string) error {
	payload, err := json.Marshal(map[string]string{
		"key":   key,
		"value": value,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/values", d.leaderAddress), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	return nil
}

func (d *driver) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/values/%s", d.leaderAddress, key), http.NoBody)
	if err != nil {
		return err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	return nil
}

func (d *driver) Update(ctx context.Context, key string, value string) error {
	payload, err := json.Marshal(map[string]string{
		"key":   key,
		"value": value,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/values", d.leaderAddress), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	return nil
}
