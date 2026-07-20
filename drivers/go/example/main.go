// Reference example (needs a Go toolchain): go run ./example
package main

import (
	"fmt"
	"os"

	meshdb "github.com/meshdb/driver-go"
)

func main() {
	port := os.Getenv("MESHDB_PORT")
	if port == "" {
		port = "4680"
	}
	db := meshdb.New("http://127.0.0.1:" + port)
	db.ExecuteAll("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
	db.Tx([]meshdb.Statement{
		{SQL: "INSERT INTO t VALUES (?, ?)", Params: []any{1, "alice"}},
		{SQL: "INSERT INTO t VALUES (?, ?)", Params: []any{2, "bob"}},
	}, 0)
	rows, _ := db.Query("SELECT id, v FROM t ORDER BY id")
	defer rows.Close()
	for rows.Next() {
		r := rows.Row()
		fmt.Println(r["id"], r["v"])
	}
	info, _ := db.Info()
	fmt.Println("info:", info)
}
