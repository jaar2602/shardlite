// Run against a live gateway: SHARDLITE_PORT=NNNN cargo run --example demo
fn main() -> Result<(), shardlite_driver::Error> {
    let port = std::env::var("SHARDLITE_PORT").unwrap_or_else(|_| "4680".into());
    let db = shardlite_driver::Client::new(&format!("http://127.0.0.1:{port}"));

    println!("info: {}", db.info()?);

    // Small query.
    let small: Vec<_> = db
        .query("SELECT id,v FROM t WHERE id<=3 ORDER BY id", 0, &[])?
        .collect::<Result<_, _>>()?;
    println!("small: {small:?}");

    // Large streaming query — count without buffering.
    let mut n = 0u64;
    let mut last = 0i64;
    for row in db.query("SELECT id FROM t ORDER BY id", 0, &[])? {
        let row = row?;
        n += 1;
        let id = row["id"].as_i64().unwrap();
        assert_eq!(id, last + 1);
        last = id;
    }
    println!("streamed rows: {n}");

    println!("execute: {}", db.execute("INSERT INTO t VALUES (?1,?2)", 0, &[serde_json::json!(999999), serde_json::json!("z")])?);
    println!("tx: {}", db.tx(vec![
        serde_json::json!({"sql":"INSERT INTO t VALUES (?1,?2)","params":[100001,"a"]}),
        serde_json::json!({"sql":"INSERT INTO t VALUES (?1,?2)","params":[100002,"b"]}),
    ], 0)?);
    println!("frames txns: {}", db.frames(0)?["transactions"]);
    Ok(())
}
