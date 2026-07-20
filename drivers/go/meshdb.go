// Package meshdb is an HTTP driver for the meshdb gateway. Standard library only, streaming
// reads.
//
//	db := meshdb.New("http://localhost:4680", meshdb.WithAuth("app", "s3cret"))
//	rows, _ := db.Query("SELECT id, v FROM t WHERE id > ?", meshdb.Params(5))
//	defer rows.Close()
//	for rows.Next() {
//	    r := rows.Row()
//	    fmt.Println(r["id"], r["v"])
//	}
//
// Query streams: rows are read from the connection one at a time via a bufio.Reader (no line
// length limit), so a million-row result costs the driver almost nothing. Auth is sent as
// Authorization: Bearer base64(user:secret) — the programmatic scheme, no browser prompt.
// Over a plaintext gateway the credential is exposed; use TLS on any untrusted network.
package meshdb

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Error is a non-2xx response from the gateway.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Message) }

type Client struct {
	base string
	auth string
	http *http.Client
}

type Option func(*Client)

// WithAuth sends Bearer base64(user:secret) with every request.
func WithAuth(user, secret string) Option {
	return func(c *Client) {
		token := base64.StdEncoding.EncodeToString([]byte(user + ":" + secret))
		c.auth = "Bearer " + token
	}
}

// WithHTTPClient overrides the underlying client (timeouts, a TLS config, a proxy).
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		base: strings.TrimRight(baseURL, "/"),
		http: &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) do(method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		msg := string(raw)
		var parsed map[string]any
		if json.Unmarshal(raw, &parsed) == nil {
			if e, ok := parsed["error"].(string); ok {
				msg = e
			}
		}
		return nil, &Error{Status: resp.StatusCode, Message: msg}
	}
	return resp, nil
}

func (c *Client) doJSON(method, path string, body any, out any) error {
	resp, err := c.do(method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// -- reads --

// Statement is one SQL statement with optional bound parameters.
type Statement struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

type queryReq struct {
	Shard       int    `json:"shard"`
	SQL         string `json:"sql"`
	Params      []any  `json:"params"`
	Consistency string `json:"consistency"`
}

// QueryOpt configures a Query.
type QueryOpt func(*queryReq)

func Shard(n int) QueryOpt          { return func(q *queryReq) { q.Shard = n } }
func Params(p ...any) QueryOpt      { return func(q *queryReq) { q.Params = p } }
func Consistency(c string) QueryOpt { return func(q *queryReq) { q.Consistency = c } }

// Rows streams a query result. Call Next in a loop, Row to read the current row, Err after,
// and Close when done (Next returning false closes automatically).
type Rows struct {
	body    io.ReadCloser
	reader  *bufio.Reader
	columns []string
	current map[string]any
	err     error
	done    bool
}

func (c *Client) Query(sql string, opts ...QueryOpt) (*Rows, error) {
	q := queryReq{SQL: sql, Params: []any{}, Consistency: "linearizable"}
	for _, o := range opts {
		o(&q)
	}
	resp, err := c.do("POST", "/v1/query", q)
	if err != nil {
		return nil, err
	}
	return &Rows{body: resp.Body, reader: bufio.NewReader(resp.Body)}, nil
}

func (r *Rows) Next() bool {
	if r.done {
		return false
	}
	for {
		line, err := r.reader.ReadBytes('\n')
		if len(line) == 0 && err != nil {
			r.finish(err)
			return false
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			if err != nil {
				r.finish(err)
				return false
			}
			continue
		}
		// A header line carries the columns; a row is a JSON array; a trailing object with
		// "error" is a mid-stream failure.
		if r.columns == nil && bytes.Contains(trimmed, []byte("\"columns\"")) {
			var h struct {
				Columns []string `json:"columns"`
			}
			if json.Unmarshal(trimmed, &h) == nil && h.Columns != nil {
				r.columns = h.Columns
				continue
			}
		}
		if trimmed[0] == '{' {
			var obj map[string]any
			if json.Unmarshal(trimmed, &obj) == nil {
				if e, ok := obj["error"].(string); ok {
					r.finish(&Error{Status: 200, Message: e})
					return false
				}
			}
		}
		var cells []any
		if err := json.Unmarshal(trimmed, &cells); err != nil {
			r.finish(err)
			return false
		}
		row := make(map[string]any, len(cells))
		for i, c := range cells {
			if i < len(r.columns) {
				row[r.columns[i]] = c
			}
		}
		r.current = row
		return true
	}
}

func (r *Rows) finish(err error) {
	if err != nil && err != io.EOF {
		r.err = err
	}
	r.done = true
	r.body.Close()
}

func (r *Rows) Row() map[string]any { return r.current }
func (r *Rows) Err() error          { return r.err }
func (r *Rows) Close() error        { r.done = true; return r.body.Close() }

// QueryAll runs a fan-out read across every shard, merged.
func (c *Client) QueryAll(sql string) (map[string]any, error) {
	var out map[string]any
	return out, c.doJSON("POST", "/v1/query_all", map[string]any{"sql": sql}, &out)
}

// Route returns the shard a key maps to.
func (c *Client) Route(key string) (int, error) {
	var out struct {
		Shard int `json:"shard"`
	}
	err := c.doJSON("POST", "/v1/route", map[string]any{"key": key}, &out)
	return out.Shard, err
}

// -- writes --

func (c *Client) Execute(sql string, opts ...QueryOpt) (map[string]any, error) {
	q := queryReq{SQL: sql, Params: []any{}}
	for _, o := range opts {
		o(&q)
	}
	var out map[string]any
	body := map[string]any{"shard": q.Shard, "sql": q.SQL, "params": q.Params}
	return out, c.doJSON("POST", "/v1/execute", body, &out)
}

// Tx applies statements atomically and durably.
func (c *Client) Tx(statements []Statement, shard int) (map[string]any, error) {
	var out map[string]any
	body := map[string]any{"shard": shard, "statements": statements}
	return out, c.doJSON("POST", "/v1/tx", body, &out)
}

func (c *Client) ExecuteAll(sql string) (map[string]any, error) {
	var out map[string]any
	return out, c.doJSON("POST", "/v1/execute_all", map[string]any{"sql": sql}, &out)
}

// -- introspection & admin --

func (c *Client) Info() (map[string]any, error)         { return c.get("/v1/info") }
func (c *Client) Cluster() (map[string]any, error)      { return c.get("/v1/cluster") }
func (c *Client) Stats() (map[string]any, error)        { return c.get("/v1/stats") }
func (c *Client) Schema(shard int) (map[string]any, error) {
	return c.get(fmt.Sprintf("/v1/schema/%d", shard))
}
func (c *Client) Frames(shard int) (map[string]any, error) {
	return c.get(fmt.Sprintf("/v1/frames/%d", shard))
}

func (c *Client) get(path string) (map[string]any, error) {
	var out map[string]any
	return out, c.doJSON("GET", path, nil, &out)
}

func (c *Client) ListUsers() ([]any, error) {
	var out struct {
		Users []any `json:"users"`
	}
	return out.Users, c.doJSON("GET", "/v1/users", nil, &out)
}

func (c *Client) CreateUser(name, secret, role string) error {
	_, err := c.do("POST", "/v1/users", map[string]any{"name": name, "secret": secret, "role": role})
	return err
}

func (c *Client) DropUser(name string) error {
	_, err := c.do("DELETE", "/v1/users/"+name, nil)
	return err
}
