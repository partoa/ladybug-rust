/// Benchmark: Arrow import of 2M nodes + 3M edges
/// Measures wall-clock time, peak RSS, and CPU time.

use arrow::array::{Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;

fn get_rss_kb() -> u64 {
    // Read from /proc/self/status
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                return parts[1].parse().unwrap_or(0);
            }
        }
    }
    0
}

fn get_cpu_times() -> (f64, f64) {
    // user, system in seconds from /proc/self/stat
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let parts: Vec<&str> = stat.split_whitespace().collect();
    if parts.len() > 15 {
        let ticks_per_sec: f64 = 100.0; // typical
        let utime = parts[13].parse::<f64>().unwrap_or(0.0) / ticks_per_sec;
        let stime = parts[14].parse::<f64>().unwrap_or(0.0) / ticks_per_sec;
        (utime, stime)
    } else {
        (0.0, 0.0)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let num_nodes: i64 = 2_000_000;
    let num_edges: i64 = 3_000_000;

    println!("=== Arrow Import Benchmark ===");
    println!("Nodes: {num_nodes}, Edges: {num_edges}");
    println!();

    let rss_start = get_rss_kb();
    let (cpu_u_start, cpu_s_start) = get_cpu_times();

    // --- Build node RecordBatch (id: INT64, name: STRING) ---
    let t0 = Instant::now();
    let ids: Vec<i64> = (0..num_nodes).collect();
    let mut name_builder = StringBuilder::with_capacity(num_nodes as usize, num_nodes as usize * 10);
    for i in 0..num_nodes {
        name_builder.append_value(format!("person_{i}"));
    }
    let node_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let node_batch = RecordBatch::try_new(
        node_schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(name_builder.finish()),
        ],
    )?;
    let build_nodes_ms = t0.elapsed().as_millis();
    let rss_after_node_batch = get_rss_kb();
    println!("[1] Build node batch:    {build_nodes_ms:>6} ms  (RSS: {} MB)", rss_after_node_batch / 1024);

    // --- Build edge RecordBatch (source: INT64, target: INT64, since: INT64) ---
    let t0 = Instant::now();
    let sources: Vec<i64> = (0..num_edges).map(|i| i % num_nodes).collect();
    let targets: Vec<i64> = (0..num_edges).map(|i| (i * 7 + 1) % num_nodes).collect();
    let sinces: Vec<i64> = (0..num_edges).map(|i| 2000 + (i % 25)).collect();
    let edge_schema = Arc::new(Schema::new(vec![
        Field::new("source", DataType::Int64, false),
        Field::new("target", DataType::Int64, false),
        Field::new("since", DataType::Int64, false),
    ]));
    let edge_batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(Int64Array::from(sources)),
            Arc::new(Int64Array::from(targets)),
            Arc::new(Int64Array::from(sinces)),
        ],
    )?;
    let build_edges_ms = t0.elapsed().as_millis();
    let rss_after_edge_batch = get_rss_kb();
    println!("[2] Build edge batch:    {build_edges_ms:>6} ms  (RSS: {} MB)", rss_after_edge_batch / 1024);

    // --- Create DB and connection ---
    let temp_dir = tempfile::tempdir()?;
    let db = lbug::Database::new(temp_dir.path().join("bench"), lbug::SystemConfig::default())?;
    let conn = lbug::Connection::new(&db)?;

    // --- Import nodes ---
    let t0 = Instant::now();
    conn.query("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));")?;
    let _arrow_id = conn.create_node_table_from_arrow("_arrow_tmp_Person", &node_batch)?;

    let copy_q = "COPY Person FROM (MATCH (t:_arrow_tmp_Person) RETURN t.id, t.name)";
    conn.query(copy_q)?;
    conn.drop_arrow_table("_arrow_tmp_Person")?;
    let import_nodes_ms = t0.elapsed().as_millis();
    let rss_after_nodes = get_rss_kb();
    println!("[3] Import 2M nodes:     {import_nodes_ms:>6} ms  (RSS: {} MB)", rss_after_nodes / 1024);

    // --- Create REL table and import edges ---
    let t0 = Instant::now();
    conn.query("CREATE REL TABLE Knows(FROM Person TO Person, since INT64);")?;
    conn.insert_arrow_rel("Knows", &edge_batch)?;
    let import_edges_ms = t0.elapsed().as_millis();
    let rss_after_edges = get_rss_kb();
    println!("[4] Import 3M edges:     {import_edges_ms:>6} ms  (RSS: {} MB)", rss_after_edges / 1024);

    // --- Verify ---
    let t0 = Instant::now();
    let mut result = conn.query("MATCH (a:Person)-[r:Knows]->(b:Person) RETURN count(*);")?;
    let row = result.next().unwrap();
    let verify_ms = t0.elapsed().as_millis();
    println!("[5] Verify count(*):     {verify_ms:>6} ms  -> {}", row[0]);

    // --- Final stats ---
    let (cpu_u_end, cpu_s_end) = get_cpu_times();
    let rss_peak = get_rss_kb();
    println!();
    println!("=== Summary ===");
    println!("Total wall time:  {} ms", build_nodes_ms + build_edges_ms + import_nodes_ms + import_edges_ms + verify_ms);
    println!("  Node batch build: {build_nodes_ms} ms");
    println!("  Edge batch build: {build_edges_ms} ms");
    println!("  Node import:      {import_nodes_ms} ms");
    println!("  Edge import:      {import_edges_ms} ms");
    println!("  Verify query:     {verify_ms} ms");
    println!("RSS start:   {} MB", rss_start / 1024);
    println!("RSS peak:    {} MB", rss_peak / 1024);
    println!("RSS delta:   {} MB", (rss_peak.saturating_sub(rss_start)) / 1024);
    println!("CPU user:    {:.2} s", cpu_u_end - cpu_u_start);
    println!("CPU system:  {:.2} s", cpu_s_end - cpu_s_start);

    // Cleanup
    drop(conn);
    drop(db);
    temp_dir.close()?;

    Ok(())
}
