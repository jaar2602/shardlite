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

	if tcpPort := os.Getenv("MESHDB_TCP_PORT"); tcpPort != "" {
		tc, err := meshdb.DialTCP("127.0.0.1:"+tcpPort, "", "")
		if err != nil {
			fmt.Println("tcp dial error:", err)
			return
		}
		defer tc.Close()
		trows, _ := tc.Query("SELECT id FROM t ORDER BY id")
		m := 0
		for trows.Next() {
			m++
		}
		fmt.Println("tcp streamed rows:", m)
	}
}
