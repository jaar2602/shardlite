// Package shardlite is an HTTP driver for the shardlite gateway. Standard library only, streaming
// reads.
//
//	db := shardlite.New("http://localhost:4680", shardlite.WithAuth("app", "s3cret"))
//	rows, _ := db.Query("SELECT id, v FROM t WHERE id > ?", shardlite.Params(5))
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
package shardlite

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

// -- Persistent TCP transport (JSON-over-TCP) --

import (
	"encoding/binary"
	"net"
	"sync"
)

// TCPClient is a persistent-connection client over shardlite's JSON-over-TCP protocol. Lower
// per-request overhead than HTTP. One request at a time per connection; guard with the mutex
// if shared. Query streams. Auth is sent once at connect; the secret crosses the wire, so use
// a trusted network or a TLS tunnel.
//
//	db, _ := shardlite.DialTCP("127.0.0.1:4620", "app", "s3cret")
//	rows, _ := db.Query("SELECT id, v FROM t")
//	for rows.Next() { fmt.Println(rows.Row()) }
type TCPClient struct {
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex
}

// DialTCP connects and, if user is non-empty, authenticates.
func DialTCP(addr, user, secret string) (*TCPClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c := &TCPClient{conn: conn, r: bufio.NewReader(conn)}
	if user != "" {
		r, err := c.call(map[string]any{"op": "auth", "name": user, "secret": secret})
		if err != nil {
			conn.Close()
			return nil, err
		}
		if ok, _ := r["ok"].(bool); !ok {
			conn.Close()
			return nil, &Error{Status: 401, Message: "authentication failed"}
		}
	}
	return c, nil
}

func (c *TCPClient) Close() error { return c.conn.Close() }

func (c *TCPClient) send(frame any) error {
	body, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := c.conn.Write(hdr[:]); err != nil {
		return err
	}
	_, err = c.conn.Write(body)
	return err
}

func (c *TCPClient) recv() (map[string]any, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	body := make([]byte, n)
	if _, err := io.ReadFull(c.r, body); err != nil {
		return nil, err
	}
	var frame map[string]any
	return frame, json.Unmarshal(body, &frame)
}

// call runs a bounded op: one request, one result frame.
func (c *TCPClient) call(frame any) (map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.send(frame); err != nil {
		return nil, err
	}
	r, err := c.recv()
	if err != nil {
		return nil, err
	}
	if e, ok := r["error"].(string); ok {
		status := 0
		if s, ok := r["status"].(float64); ok {
			status = int(s)
		}
		return nil, &Error{Status: status, Message: e}
	}
	result, _ := r["result"].(map[string]any)
	return result, nil
}

// TCPRows streams a JSON-TCP query result.
type TCPRows struct {
	c       *TCPClient
	columns []string
	current map[string]any
	err     error
	done    bool
}

func (c *TCPClient) Query(sql string, opts ...QueryOpt) (*TCPRows, error) {
	q := queryReq{SQL: sql, Params: []any{}, Consistency: "linearizable"}
	for _, o := range opts {
		o(&q)
	}
	c.mu.Lock()
	if err := c.send(map[string]any{
		"op": "query", "shard": q.Shard, "sql": q.SQL,
		"params": q.Params, "consistency": q.Consistency,
	}); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	// The mutex is held for the whole stream; Close/Next release it at the terminal frame.
	return &TCPRows{c: c}, nil
}

func (r *TCPRows) Next() bool {
	if r.done {
		return false
	}
	f, err := r.c.recv()
	if err != nil {
		r.finish(err)
		return false
	}
	if cols, ok := f["columns"].([]any); ok {
		r.columns = make([]string, len(cols))
		for i, c := range cols {
			r.columns[i], _ = c.(string)
		}
		return r.Next()
	}
	if row, ok := f["row"].([]any); ok {
		m := make(map[string]any, len(row))
		for i, v := range row {
			if i < len(r.columns) {
				m[r.columns[i]] = v
			}
		}
		r.current = m
		return true
	}
	if _, ok := f["end"]; ok {
		r.finish(nil)
		return false
	}
	if e, ok := f["error"].(string); ok {
		r.finish(&Error{Status: 200, Message: e})
		return false
	}
	r.finish(nil)
	return false
}

func (r *TCPRows) finish(err error) {
	if err != nil {
		r.err = err
	}
	r.done = true
	r.c.mu.Unlock()
}

func (r *TCPRows) Row() map[string]any { return r.current }
func (r *TCPRows) Err() error          { return r.err }

func (c *TCPClient) Execute(sql string, shard int, params ...any) (map[string]any, error) {
	if params == nil {
		params = []any{}
	}
	return c.call(map[string]any{"op": "execute", "shard": shard, "sql": sql, "params": params})
}

func (c *TCPClient) Tx(statements []Statement, shard int) (map[string]any, error) {
	return c.call(map[string]any{"op": "tx", "shard": shard, "statements": statements})
}

func (c *TCPClient) Info() (map[string]any, error)    { return c.call(map[string]any{"op": "info"}) }
func (c *TCPClient) Cluster() (map[string]any, error) { return c.call(map[string]any{"op": "cluster"}) }
func (c *TCPClient) Frames(shard int) (map[string]any, error) {
	return c.call(map[string]any{"op": "frames", "shard": shard})
}
