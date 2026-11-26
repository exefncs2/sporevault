//! SporeVault v0.4.0 — Bioluminescent Mycelium Memory
//! 不可變遞增 ID · 強制毫秒進出時間 (ms) · 叢集 · #hashtag 插旗 · 小 SELECT 查詢 · 真分頁

mod config;
mod core;

use crate::config::{
    default_vault_path, DEFAULT_BINARY_ADDR, DEFAULT_HTTP_ADDR, DEFAULT_PAGE_SIZE, DEFAULT_UI_ADDR,
    MIN_DISPLAY_SCORE,
};
use crate::core::{
    format_taipei_ms, now_epoch_ms, parse_inline_hashtags, DirectConduitClient, SporeVault,
};
use std::collections::BTreeMap;
use std::io::{self, BufRead, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn print_usage() {
    println!("🍄 SporeVault v0.4.0 — 菌絲記憶庫（直連 SporeFlow :9090）");
    println!();
    println!("用法：");
    println!("  sporevault ui                      開啟本機蘑菇記憶庫介面（http://{DEFAULT_UI_ADDR}）");
    println!("  sporevault ingest <檔案或資料夾>   匯入文字/程式碼並自動分叢集＋插旗＋同步 SporeFlow");
    println!("  sporevault add <文字含 #hashtag>   新增單筆筆記（鎖死 ID＋ms時間）並同步 SporeFlow");
    println!("  sporevault query <查詢詞/#hashtag> 透過 :9090 6 槽矩陣流撈出 Top-K 原文與進出時間 (ms)");
    println!("  sporevault repl                    開啟常駐 :9090 長連線互動搜尋模式");
    println!("  sporevault list                    列出叢集與 #hashtag 旗幟統計");
    println!("  sporevault sync                    重開機後將本地 vault.svdb 秒刷回 SporeFlow");
    println!("  sporevault status                  查看資料庫狀態與 SporeFlow 連線");
    println!("  sporevault clear                   清空本地資料庫");
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        print_usage();
        return;
    }

    let vault_path = default_vault_path();
    let cmd = args[0].as_str();

    match cmd {
        "ui" => run_vault_web_ui(vault_path),
        "ingest" => {
            if args.len() < 2 {
                eprintln!("請提供要匯入的檔案或資料夾路徑");
                std::process::exit(1);
            }
            let target = Path::new(&args[1]);
            let in_ms = now_epoch_ms();
            let t0 = Instant::now();
            let mut vault = SporeVault::load(&vault_path).unwrap_or_else(|e| {
                eprintln!("讀取資料庫失敗：{e}");
                std::process::exit(1);
            });
            let added = vault.ingest_path(target).unwrap_or_else(|e| {
                eprintln!("匯入失敗：{e}");
                std::process::exit(1);
            });
            vault.save(&vault_path).expect("寫入 vault.svdb 失敗");
            let out_ms = now_epoch_ms();
            let elapsed_ms = t0.elapsed().as_secs_f64() * 1_000.0;
            println!(
                "🍄 已匯入 {} 筆（共 {} 筆，ID #0..#{} 鎖死）| 進 {} ➔ 出 {} | {:.3} ms",
                added,
                vault.records.len(),
                vault.records.len().saturating_sub(1),
                format_taipei_ms(in_ms),
                format_taipei_ms(out_ms),
                elapsed_ms
            );
            sync_and_report(&vault);
        }
        "add" => {
            if args.len() < 2 {
                eprintln!("請提供文字內容，例如：sporevault add \"重開機驗證 #bazzite #rust\"");
                std::process::exit(1);
            }
            let text = args[1..].join(" ");
            let mut vault = SporeVault::load(&vault_path).unwrap_or_else(|_| SporeVault::new());
            let id = vault.add_text("manual-note", &text);
            vault.save(&vault_path).expect("寫入 vault.svdb 失敗");
            if let Some(r) = vault.get(id) {
                println!(
                    "🍄 已鎖定 ID #{} | 叢集 [{}] | 旗幟 {:?} | 存入 {}",
                    id,
                    r.cluster,
                    r.tags,
                    format_taipei_ms(r.created_at_ms)
                );
            }
            sync_and_report(&vault);
        }
        "sync" => {
            let vault = SporeVault::load(&vault_path).unwrap_or_else(|e| {
                eprintln!("讀取資料庫失敗：{e}");
                std::process::exit(1);
            });
            sync_and_report(&vault);
        }
        "query" => {
            if args.len() < 2 {
                eprintln!("請提供查詢詞，例如：sporevault query \"#prot_read 唯讀鎖定\"");
                std::process::exit(1);
            }
            let q = args[1..].join(" ");
            let vault = SporeVault::load(&vault_path).unwrap_or_else(|e| {
                eprintln!("讀取資料庫失敗：{e}");
                std::process::exit(1);
            });
            let mut client = DirectConduitClient::connect(DEFAULT_BINARY_ADDR).unwrap_or_else(|e| {
                eprintln!("無法直連 SporeFlow {DEFAULT_BINARY_ADDR}：{e}");
                std::process::exit(1);
            });
            run_single_query(&vault, &mut client, &q);
        }
        "repl" => {
            let vault = SporeVault::load(&vault_path).unwrap_or_else(|e| {
                eprintln!("讀取資料庫失敗：{e}");
                std::process::exit(1);
            });
            let mut client = DirectConduitClient::connect(DEFAULT_BINARY_ADDR).unwrap_or_else(|e| {
                eprintln!("無法直連 SporeFlow {DEFAULT_BINARY_ADDR}：{e}");
                std::process::exit(1);
            });
            println!(
                "🍄 SporeVault 長連線模式（庫內 {} 筆，強制 ms，輸入 exit 離開）",
                vault.records.len()
            );
            let stdin = io::stdin();
            for line in stdin.lock().lines().flatten() {
                let q = line.trim();
                if q.is_empty() {
                    continue;
                }
                if q == "exit" || q == "quit" {
                    break;
                }
                run_single_query(&vault, &mut client, q);
                print!("> ");
                let _ = io::stdout().flush();
            }
        }
        "list" => {
            let vault = SporeVault::load(&vault_path).unwrap_or_else(|_| SporeVault::new());
            let mut clusters: BTreeMap<String, usize> = BTreeMap::new();
            let mut tags: BTreeMap<String, usize> = BTreeMap::new();
            for rec in &vault.records {
                *clusters.entry(rec.cluster.clone()).or_insert(0) += 1;
                for t in &rec.tags {
                    *tags.entry(t.clone()).or_insert(0) += 1;
                }
            }
            println!("🍄 菌絲叢集統計（共 {} 筆）：", vault.records.len());
            for (c, cnt) in clusters {
                println!("  ├─ {:<18} : {:>4} 筆", c, cnt);
            }
            println!("🚩 熱門 #hashtag 旗幟：");
            let mut tag_list: Vec<(String, usize)> = tags.into_iter().collect();
            tag_list.sort_by(|a, b| b.1.cmp(&a.1));
            for (t, cnt) in tag_list.into_iter().take(12) {
                println!("  ├─ {:<18} : {:>4} 筆", t, cnt);
            }
        }
        "status" => {
            let vault = SporeVault::load(&vault_path).unwrap_or_else(|_| SporeVault::new());
            let file_bytes = std::fs::metadata(&vault_path).map(|m| m.len()).unwrap_or(0);
            let conduit_ok = DirectConduitClient::connect(DEFAULT_BINARY_ADDR).is_ok();
            println!("🍄 SporeVault v0.4.0 狀態：");
            println!(
                "  ├─ 菌絲記憶檔   : {} ({:.1} KB)",
                vault_path.display(),
                file_bytes as f64 / 1024.0
            );
            println!("  ├─ 鎖定紀錄數   : {} 筆", vault.records.len());
            println!(
                "  ├─ SporeFlow    : {} ({})",
                DEFAULT_BINARY_ADDR,
                if conduit_ok { "✅ 直通正常" } else { "❌ 未啟動" }
            );
            println!("  └─ 蘑菇介面     : http://{DEFAULT_UI_ADDR}");
        }
        "clear" => {
            if vault_path.exists() {
                let _ = std::fs::remove_file(&vault_path);
            }
            println!("🧹 已清空本地菌絲記憶檔：{}", vault_path.display());
        }
        _ => print_usage(),
    }
}

fn sync_and_report(vault: &SporeVault) {
    let in_ms = now_epoch_ms();
    match vault.sync_to_sporeflow(DEFAULT_HTTP_ADDR) {
        Ok((cnt, us, body)) => {
            let out_ms = now_epoch_ms();
            println!(
                "⚡ 同步 {} 筆至 SporeFlow | 進 {} ➔ 出 {} | {:.3} ms ➔ {}",
                cnt,
                format_taipei_ms(in_ms),
                format_taipei_ms(out_ms),
                us / 1_000.0,
                body
            );
        }
        Err(e) => eprintln!("⚠️ 本地已存檔，但同步至 SporeFlow 失敗：{e}"),
    }
}

fn rank_batch_hits(batch: &crate::core::ConduitBatchHit) -> Vec<(u32, f32)> {
    let mut seen: Vec<(u32, f32)> = Vec::new();
    for i in 0..batch.ids.len() {
        let id = batch.ids[i];
        let sc = batch.scores[i];
        if sc < MIN_DISPLAY_SCORE {
            continue;
        }
        if let Some(existing) = seen.iter_mut().find(|(eid, _)| *eid == id) {
            if sc > existing.1 {
                existing.1 = sc;
            }
        } else {
            seen.push((id, sc));
        }
    }
    seen.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    seen
}

fn run_single_query(vault: &SporeVault, client: &mut DirectConduitClient, q: &str) {
    let inline_tags = parse_inline_hashtags(q);
    match client.query_text_topk(q) {
        Ok(batch) => {
            let ranked = rank_batch_hits(&batch);
            println!(
                "⚡ :9090 | 進 {} ➔ 出 {} | 總 {:.3} ms（核心 {:.3} ms · 歸零 {:.3} ms）",
                format_taipei_ms(batch.in_epoch_ms),
                format_taipei_ms(batch.out_epoch_ms),
                batch.roundtrip_ms,
                batch.kernel_compute_ms,
                batch.kernel_zero_ms
            );
            let mut shown = 0usize;
            for (id, score) in ranked {
                if let Some(rec) = vault.get(id) {
                    if !inline_tags.is_empty()
                        && !inline_tags
                            .iter()
                            .all(|t| rec.tags.iter().any(|rt| rt == t))
                    {
                        continue;
                    }
                    shown += 1;
                    println!(
                        "  [Top-{}] 🔒#{} | [{}] | {:.4} | {} | {:?}",
                        shown, id, rec.cluster, score, rec.source, rec.tags
                    );
                    for line in rec.text.lines().take(3) {
                        println!("          {line}");
                    }
                    if shown >= 3 {
                        break;
                    }
                }
            }
            if shown == 0 {
                println!("   （無符合條件與門檻 {:.2} 之片段）", MIN_DISPLAY_SCORE);
            }
        }
        Err(e) => eprintln!("❌ :9090 查詢失敗：{e}"),
    }
}

// ============================================================================
// 小 SELECT 查詢解析器（支援簡寫與類 SQL：SELECT * WHERE cluster=docs tag=#kvm id>=10 LIMIT 15）
// ============================================================================

struct MiniSelectQuery {
    cluster: Option<String>,
    tag: Option<String>,
    file: Option<String>,
    kw: Option<String>,
    min_id: Option<u32>,
    max_id: Option<u32>,
    limit: usize,
}

fn parse_mini_select(expr: &str) -> MiniSelectQuery {
    let mut q = MiniSelectQuery {
        cluster: None,
        tag: None,
        file: None,
        kw: None,
        min_id: None,
        max_id: None,
        limit: 20,
    };
    let tokens: Vec<&str> = expr.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let raw = tokens[i].trim_matches(|c| c == ',' || c == ';');
        let low = raw.to_ascii_lowercase();
        if low == "select" || low == "*" || low == "where" || low == "and" || low == "from" || low == "vault" {
            i += 1;
            continue;
        }
        if low == "limit" && i + 1 < tokens.len() {
            if let Ok(v) = tokens[i + 1].trim_matches(';').parse::<usize>() {
                q.limit = v.clamp(1, 100);
            }
            i += 2;
            continue;
        }
        if let Some(val) = low.strip_prefix("limit=") {
            if let Ok(v) = val.parse::<usize>() {
                q.limit = v.clamp(1, 100);
            }
        } else if let Some(val) = raw.strip_prefix("id>=").or_else(|| raw.strip_prefix("ID>=")) {
            q.min_id = val.parse().ok();
        } else if let Some(val) = raw.strip_prefix("id<=").or_else(|| raw.strip_prefix("ID<=")) {
            q.max_id = val.parse().ok();
        } else if let Some(val) = raw.strip_prefix("id=").or_else(|| raw.strip_prefix("ID=")) {
            if let Ok(v) = val.parse::<u32>() {
                q.min_id = Some(v);
                q.max_id = Some(v);
            }
        } else if let Some(idx) = raw.find('=') {
            let key = raw[..idx].to_ascii_lowercase();
            let val = raw[idx + 1..].trim_matches(|c| c == '\'' || c == '"');
            match key.as_str() {
                "cluster" | "c" => q.cluster = Some(val.to_string()),
                "tag" | "t" => {
                    let t = if val.starts_with('#') {
                        val.to_lowercase()
                    } else {
                        format!("#{}", val.to_lowercase())
                    };
                    q.tag = Some(t);
                }
                "file" | "f" => q.file = Some(val.to_string()),
                "kw" | "text" | "like" => q.kw = Some(val.to_lowercase()),
                _ => {}
            }
        } else if raw.starts_with('#') {
            q.tag = Some(raw.to_lowercase());
        } else {
            q.kw = Some(raw.to_lowercase());
        }
        i += 1;
    }
    q
}

// ============================================================================
// 極簡本機發光蘑菇網頁伺服器（127.0.0.1:8081）
// ============================================================================

fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => {}
            '\t' => out.push_str("  "),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query_param(path: &str, key: &str) -> Option<String> {
    let qpart = path.split('?').nth(1)?;
    for pair in qpart.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next()? == key {
            return Some(url_decode(kv.next().unwrap_or("")));
        }
    }
    None
}

fn send_http(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) {
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.as_bytes().len()
    );
    let _ = stream.write_all(resp.as_bytes());
    let _ = stream.flush();
    let _ = stream.shutdown(Shutdown::Write);
}

fn format_tags_json(tags: &[String]) -> String {
    tags.iter()
        .map(|t| format!("\"{}\"", escape_json(t)))
        .collect::<Vec<_>>()
        .join(",")
}

fn run_vault_web_ui(vault_path: PathBuf) {
    let vault = SporeVault::load(&vault_path).unwrap_or_else(|_| SporeVault::new());
    let state = Arc::new(Mutex::new(vault));
    let listener = TcpListener::bind(DEFAULT_UI_ADDR)
        .unwrap_or_else(|e| panic!("無法綁定 {DEFAULT_UI_ADDR}: {e}"));

    println!("==================================================================");
    println!(" 🍄 SporeVault v0.4.0 — Bioluminescent Mycelium Memory");
    println!("    ├─ 瀏覽器網址 : http://{DEFAULT_UI_ADDR} (僅限本機)");
    println!("    ├─ 強制時間   : 毫秒 (ms) 進出時間戳 (UTC+8)");
    println!("    └─ 直連加速   : tcp://{DEFAULT_BINARY_ADDR}");
    println!("==================================================================");

    for stream in listener.incoming().flatten() {
        let st = Arc::clone(&state);
        let vp = vault_path.clone();
        std::thread::spawn(move || handle_ui_client(stream, st, vp));
    }
}

fn handle_ui_client(mut stream: TcpStream, state: Arc<Mutex<SporeVault>>, vault_path: PathBuf) {
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
    let mut buf = vec![0u8; 256 * 1024];
    let mut total = 0usize;
    let mut h_end: Option<usize> = None;
    let mut expected = 0usize;

    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                if h_end.is_none() {
                    let s = String::from_utf8_lossy(&buf[..total]);
                    if let Some(pos) = s.find("\r\n\r\n") {
                        h_end = Some(pos);
                        let mut clen = 0usize;
                        for line in s[..pos].lines() {
                            if line.to_ascii_lowercase().starts_with("content-length:") {
                                clen = line
                                    .split(':')
                                    .nth(1)
                                    .and_then(|v| v.trim().parse().ok())
                                    .unwrap_or(0);
                            }
                        }
                        expected = pos + 4 + clen;
                    }
                }
                if h_end.is_some() && total >= expected {
                    break;
                }
                if total >= buf.len() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    if total == 0 {
        return;
    }

    let req_str = String::from_utf8_lossy(&buf[..total]);
    let first_line = req_str.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");
    let body = h_end
        .map(|p| String::from_utf8_lossy(&buf[(p + 4).min(total)..total]).into_owned())
        .unwrap_or_default();

    // 1. 分頁瀏覽 API（含叢集、#hashtag、檔案與真分頁）
    if method == "GET" && path.starts_with("/api/records") {
        let in_ms = now_epoch_ms();
        let t0 = Instant::now();
        let cluster_filter = query_param(path, "cluster").unwrap_or_default();
        let tag_filter = query_param(path, "tag").unwrap_or_default().to_lowercase();
        let file_filter = query_param(path, "file").unwrap_or_default();
        let kw_filter = query_param(path, "kw").unwrap_or_default().to_lowercase();
        let page: usize = query_param(path, "page")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
            .max(1);
        let page_size: usize = query_param(path, "page_size")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_PAGE_SIZE)
            .clamp(5, 100);

        let guard = state.lock().unwrap();
        let mut clusters_map: BTreeMap<String, usize> = BTreeMap::new();
        let mut tags_map: BTreeMap<String, usize> = BTreeMap::new();
        let mut files_map: BTreeMap<String, usize> = BTreeMap::new();
        let mut matched_indices: Vec<usize> = Vec::new();

        for (idx, rec) in guard.records.iter().enumerate() {
            *clusters_map.entry(rec.cluster.clone()).or_insert(0) += 1;
            for t in &rec.tags {
                *tags_map.entry(t.clone()).or_insert(0) += 1;
            }
            let fname = rec.source.split('#').next().unwrap_or(&rec.source);
            *files_map.entry(fname.to_string()).or_insert(0) += 1;

            if !cluster_filter.is_empty() && rec.cluster != cluster_filter {
                continue;
            }
            if !tag_filter.is_empty() && !rec.tags.iter().any(|t| t == &tag_filter) {
                continue;
            }
            if !file_filter.is_empty() && fname != file_filter {
                continue;
            }
            if !kw_filter.is_empty()
                && !rec.text.to_lowercase().contains(&kw_filter)
                && !rec.source.to_lowercase().contains(&kw_filter)
                && !rec.tags.iter().any(|t| t.contains(&kw_filter))
            {
                continue;
            }
            matched_indices.push(idx);
        }

        let total_records = guard.records.len();
        let total_filtered = matched_indices.len();
        let total_pages = total_filtered.div_ceil(page_size).max(1);
        let clamped_page = page.min(total_pages);
        let start = (clamped_page - 1) * page_size;
        let end = (start + page_size).min(total_filtered);

        let mut items_json = Vec::with_capacity(end.saturating_sub(start));
        for &idx in &matched_indices[start..end] {
            let rec = &guard.records[idx];
            items_json.push(format!(
                "{{\"id\":{},\"cluster\":\"{}\",\"tags\":[{}],\"source\":\"{}\",\"created_ms\":{},\"created_str\":\"{}\",\"text\":\"{}\"}}",
                rec.id,
                escape_json(&rec.cluster),
                format_tags_json(&rec.tags),
                escape_json(&rec.source),
                rec.created_at_ms,
                format_taipei_ms(rec.created_at_ms),
                escape_json(&rec.text)
            ));
        }

        let clusters_json: Vec<String> = clusters_map
            .into_iter()
            .map(|(k, v)| format!("{{\"name\":\"{}\",\"count\":{v}}}", escape_json(&k)))
            .collect();

        let mut tag_pairs: Vec<(String, usize)> = tags_map.into_iter().collect();
        tag_pairs.sort_by(|a, b| b.1.cmp(&a.1));
        let tags_json: Vec<String> = tag_pairs
            .into_iter()
            .take(20)
            .map(|(k, v)| format!("{{\"name\":\"{}\",\"count\":{v}}}", escape_json(&k)))
            .collect();

        let files_json: Vec<String> = files_map
            .into_iter()
            .map(|(k, v)| format!("{{\"name\":\"{}\",\"count\":{v}}}", escape_json(&k)))
            .collect();
        drop(guard);

        let elapsed_ms = t0.elapsed().as_secs_f64() * 1_000.0;
        let out_ms = now_epoch_ms();
        let json = format!(
            "{{\"total\":{},\"total_filtered\":{},\"page\":{},\"page_size\":{},\"total_pages\":{},\"in_str\":\"{}\",\"out_str\":\"{}\",\"elapsed_ms\":{:.3},\"clusters\":[{}],\"tags\":[{}],\"files\":[{}],\"records\":[{}]}}",
            total_records,
            total_filtered,
            clamped_page,
            page_size,
            total_pages,
            format_taipei_ms(in_ms),
            format_taipei_ms(out_ms),
            elapsed_ms,
            clusters_json.join(","),
            tags_json.join(","),
            files_json.join(","),
            items_json.join(",")
        );
        send_http(&mut stream, "200 OK", "application/json; charset=utf-8", &json);
        return;
    }

    // 2. 小 SELECT 結構化查詢 API（免完整 SQL，直接篩選 cluster / tag / id 區間 / kw / limit）
    if method == "GET" && path.starts_with("/api/select") {
        let expr = query_param(path, "expr").unwrap_or_default();
        let in_ms = now_epoch_ms();
        let t0 = Instant::now();
        let parsed = parse_mini_select(&expr);

        let guard = state.lock().unwrap();
        let mut matched_total = 0usize;
        let mut items_json = Vec::new();
        for rec in &guard.records {
            if let Some(min_id) = parsed.min_id {
                if rec.id < min_id {
                    continue;
                }
            }
            if let Some(max_id) = parsed.max_id {
                if rec.id > max_id {
                    continue;
                }
            }
            if let Some(ref c) = parsed.cluster {
                if &rec.cluster != c {
                    continue;
                }
            }
            if let Some(ref t) = parsed.tag {
                if !rec.tags.iter().any(|rt| rt == t) {
                    continue;
                }
            }
            if let Some(ref f) = parsed.file {
                let fname = rec.source.split('#').next().unwrap_or(&rec.source);
                if !fname.to_lowercase().contains(&f.to_lowercase()) {
                    continue;
                }
            }
            if let Some(ref k) = parsed.kw {
                if !rec.text.to_lowercase().contains(k) && !rec.source.to_lowercase().contains(k) {
                    continue;
                }
            }
            matched_total += 1;
            if items_json.len() < parsed.limit {
                items_json.push(format!(
                    "{{\"id\":{},\"cluster\":\"{}\",\"tags\":[{}],\"source\":\"{}\",\"created_str\":\"{}\",\"text\":\"{}\"}}",
                    rec.id,
                    escape_json(&rec.cluster),
                    format_tags_json(&rec.tags),
                    escape_json(&rec.source),
                    format_taipei_ms(rec.created_at_ms),
                    escape_json(&rec.text)
                ));
            }
        }
        drop(guard);

        let elapsed_ms = t0.elapsed().as_secs_f64() * 1_000.0;
        let out_ms = now_epoch_ms();
        let json = format!(
            "{{\"matched_total\":{},\"returned\":{},\"in_str\":\"{}\",\"out_str\":\"{}\",\"elapsed_ms\":{:.3},\"records\":[{}]}}",
            matched_total,
            items_json.len(),
            format_taipei_ms(in_ms),
            format_taipei_ms(out_ms),
            elapsed_ms,
            items_json.join(",")
        );
        send_http(&mut stream, "200 OK", "application/json; charset=utf-8", &json);
        return;
    }

    // 3. :9090 向量直連搜尋 API
    if method == "GET" && path.starts_with("/api/search") {
        let q = query_param(path, "q").unwrap_or_default();
        let cluster_filter = query_param(path, "cluster").unwrap_or_default();
        let mut tag_filter = query_param(path, "tag").unwrap_or_default().to_lowercase();
        let inline_tags = parse_inline_hashtags(&q);
        if tag_filter.is_empty() && !inline_tags.is_empty() {
            tag_filter = inline_tags[0].clone();
        }
        if q.trim().is_empty() {
            send_http(&mut stream, "200 OK", "application/json; charset=utf-8", "{\"hits\":[],\"elapsed_ms\":0}");
            return;
        }
        let mut client = match DirectConduitClient::connect(DEFAULT_BINARY_ADDR) {
            Ok(c) => c,
            Err(e) => {
                let err = format!("{{\"error\":\"無法連線 SporeFlow :9090 ({})\"}}", escape_json(&e.to_string()));
                send_http(&mut stream, "502 Bad Gateway", "application/json; charset=utf-8", &err);
                return;
            }
        };
        match client.query_text_topk(&q) {
            Ok(batch) => {
                let ranked = rank_batch_hits(&batch);
                let guard = state.lock().unwrap();
                let mut hits_json = Vec::new();
                for (id, score) in ranked {
                    if let Some(rec) = guard.get(id) {
                        if !cluster_filter.is_empty() && rec.cluster != cluster_filter {
                            continue;
                        }
                        if !tag_filter.is_empty() && !rec.tags.iter().any(|t| t == &tag_filter) {
                            continue;
                        }
                        hits_json.push(format!(
                            "{{\"id\":{},\"cluster\":\"{}\",\"tags\":[{}],\"score\":{:.4},\"source\":\"{}\",\"created_str\":\"{}\",\"text\":\"{}\"}}",
                            rec.id,
                            escape_json(&rec.cluster),
                            format_tags_json(&rec.tags),
                            score,
                            escape_json(&rec.source),
                            format_taipei_ms(rec.created_at_ms),
                            escape_json(&rec.text)
                        ));
                        if hits_json.len() >= 5 {
                            break;
                        }
                    }
                }
                drop(guard);
                let json = format!(
                    "{{\"in_str\":\"{}\",\"out_str\":\"{}\",\"elapsed_ms\":{:.3},\"kernel_ms\":{:.3},\"zero_ms\":{:.3},\"applied_cluster\":\"{}\",\"applied_tag\":\"{}\",\"hits\":[{}]}}",
                    format_taipei_ms(batch.in_epoch_ms),
                    format_taipei_ms(batch.out_epoch_ms),
                    batch.roundtrip_ms,
                    batch.kernel_compute_ms,
                    batch.kernel_zero_ms,
                    escape_json(&cluster_filter),
                    escape_json(&tag_filter),
                    hits_json.join(",")
                );
                send_http(&mut stream, "200 OK", "application/json; charset=utf-8", &json);
            }
            Err(e) => {
                let err = format!("{{\"error\":\"{}\"}}", escape_json(&e.to_string()));
                send_http(&mut stream, "500 Internal Server Error", "application/json; charset=utf-8", &err);
            }
        }
        return;
    }

    // 4. 新增筆記（鎖定遞增 ID ＋ 自動插旗 ＋ 同步 SporeFlow）
    if method == "POST" && path.starts_with("/api/add") {
        let text = body.trim();
        if text.is_empty() {
            send_http(&mut stream, "400 Bad Request", "application/json", "{\"error\":\"內容不得為空\"}");
            return;
        }
        let cluster_opt = query_param(path, "cluster");
        let in_ms = now_epoch_ms();
        let t0 = Instant::now();
        let (id, cluster, tags_json, created_str, sync_ms) = {
            let mut guard = state.lock().unwrap();
            let id = guard.add_text_with_cluster("web-note", text, cluster_opt.as_deref());
            let _ = guard.save(&vault_path);
            let (cl, tj, cs) = guard
                .get(id)
                .map(|r| {
                    (
                        r.cluster.clone(),
                        format_tags_json(&r.tags),
                        format_taipei_ms(r.created_at_ms),
                    )
                })
                .unwrap_or_default();
            let sync_ms = guard
                .sync_to_sporeflow(DEFAULT_HTTP_ADDR)
                .map(|(_, us, _)| us / 1_000.0)
                .unwrap_or(0.0);
            (id, cl, tj, cs, sync_ms)
        };
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1_000.0;
        let out_ms = now_epoch_ms();
        let json = format!(
            "{{\"id\":{},\"cluster\":\"{}\",\"tags\":[{}],\"created_str\":\"{}\",\"in_str\":\"{}\",\"out_str\":\"{}\",\"elapsed_ms\":{:.3},\"sync_ms\":{:.3}}}",
            id,
            escape_json(&cluster),
            tags_json,
            created_str,
            format_taipei_ms(in_ms),
            format_taipei_ms(out_ms),
            elapsed_ms,
            sync_ms
        );
        send_http(&mut stream, "200 OK", "application/json; charset=utf-8", &json);
        return;
    }

    send_http(&mut stream, "200 OK", "text/html; charset=utf-8", VAULT_UI_HTML);
}

const VAULT_UI_HTML: &str = r##"<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🍄 SporeVault</title>
<style>
  :root {
    --forest:#070d0b;
    --moss:#0d1714;
    --cap:#12221d;
    --border:#1f3b32;
    --glow:#34d399;
    --teal:#2dd4bf;
    --spore:#fbbf24;
    --lilac:#c084fc;
    --text:#e6f4ef;
    --muted:#8ba99f;
  }
  * { box-sizing:border-box; margin:0; padding:0; font-family: 'Inter', system-ui, -apple-system, monospace; }
  body {
    background:
      radial-gradient(circle at 12% 10%, rgba(45,212,191,0.12), transparent 36%),
      radial-gradient(circle at 88% 18%, rgba(52,211,153,0.10), transparent 40%),
      radial-gradient(circle at 50% 95%, rgba(192,132,252,0.08), transparent 45%),
      var(--forest);
    color:var(--text);
    min-height:100vh;
    padding:18px;
    max-width:1240px;
    margin:0 auto;
    display:flex;
    flex-direction:column;
    gap:14px;
  }
  header {
    display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;
    background:linear-gradient(135deg, rgba(18,34,29,0.92), rgba(10,20,17,0.92));
    border:1px solid var(--border);
    border-radius:16px;
    padding:14px 18px;
    box-shadow:0 8px 28px rgba(0,0,0,0.45), inset 0 1px 0 rgba(52,211,153,0.15);
  }
  .brand { display:flex; align-items:center; gap:12px; }
  .brand h1 { font-size:1.35rem; letter-spacing:0.5px; color:var(--glow); text-shadow:0 0 18px rgba(52,211,153,0.35); }
  .brand .sub { font-size:0.74rem; color:var(--muted); margin-top:2px; }
  .pills { display:flex; gap:8px; flex-wrap:wrap; }
  .pill {
    font-size:0.73rem; padding:4px 11px; border-radius:999px;
    background:rgba(52,211,153,0.10); border:1px solid rgba(52,211,153,0.45); color:#a7f3d0;
  }
  .pill.gold { background:rgba(251,191,36,0.10); border-color:rgba(251,191,36,0.45); color:#fde68a; }
  .card {
    background:rgba(13,23,20,0.88);
    border:1px solid var(--border);
    border-radius:14px;
    padding:14px;
    display:flex; flex-direction:column; gap:10px;
    box-shadow:0 6px 22px rgba(0,0,0,0.35);
  }
  .section-title { font-size:0.84rem; font-weight:700; color:var(--teal); display:flex; align-items:center; gap:6px; }
  .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  input, select {
    background:#07100d; color:var(--text); border:1px solid var(--border);
    border-radius:9px; padding:8px 11px; font-size:0.84rem; outline:none; transition:border-color 0.15s;
  }
  input:focus, select:focus { border-color:var(--glow); box-shadow:0 0 0 2px rgba(52,211,153,0.18); }
  input[type=text] { flex:1; min-width:210px; }
  button {
    background:linear-gradient(135deg, #10b981, #14b8a6);
    color:#04241a; font-weight:700; border:none; padding:8px 14px; border-radius:9px;
    cursor:pointer; font-size:0.82rem; transition:transform 0.1s, box-shadow 0.15s;
  }
  button:hover { box-shadow:0 0 16px rgba(52,211,153,0.4); transform:translateY(-1px); }
  button.sec { background:#11221d; color:#cce7de; border:1px solid var(--border); font-weight:600; }
  button.sec:hover { border-color:var(--teal); box-shadow:none; transform:none; }
  button.chip-btn { background:#091512; color:#99f6e4; border:1px solid #1e3e34; padding:4px 10px; font-size:0.74rem; border-radius:999px; font-weight:600; }
  button.chip-btn:hover { border-color:var(--glow); color:#ecfdf5; }
  button:disabled { opacity:0.35; cursor:not-allowed; transform:none; box-shadow:none; }
  .timebar {
    font-size:0.75rem; color:var(--spore); background:#07100d;
    border:1px solid rgba(251,191,36,0.28); padding:6px 11px; border-radius:8px; font-family:monospace;
  }
  .item {
    background:#08120f; border:1px solid #1b342c; border-radius:11px; padding:11px 12px;
    display:flex; flex-direction:column; gap:6px; transition:border-color 0.15s;
  }
  .item:hover { border-color:rgba(52,211,153,0.55); }
  .meta { display:flex; justify-content:space-between; flex-wrap:wrap; gap:6px; font-size:0.76rem; color:var(--teal); align-items:center; }
  .badge-cl { background:rgba(192,132,252,0.14); border:1px solid rgba(192,132,252,0.5); color:#e9d5ff; padding:1px 8px; border-radius:999px; font-size:0.7rem; }
  .badge-tag { background:rgba(45,212,191,0.12); border:1px solid rgba(45,212,191,0.45); color:#99f6e4; padding:1px 8px; border-radius:999px; font-size:0.7rem; cursor:pointer; }
  .badge-tag:hover { background:rgba(52,211,153,0.25); }
  .ts { color:var(--muted); font-size:0.72rem; }
  pre { white-space:pre-wrap; word-break:break-word; font-size:0.81rem; color:#d5ebe3; line-height:1.48; }
  .grid { display:grid; grid-template-columns: 265px 1fr; gap:14px; }
  @media (max-width:800px) { .grid { grid-template-columns:1fr; } }
  .side-btn {
    width:100%; text-align:left; background:#08120f; color:var(--text); border:1px solid #1b342c;
    padding:6px 10px; border-radius:8px; font-size:0.77rem; display:flex; justify-content:space-between;
    margin-bottom:5px; font-weight:500;
  }
  .side-btn.active { border-color:var(--glow); background:rgba(52,211,153,0.14); color:#6ee7b7; font-weight:700; }
  .tag-cloud { display:flex; flex-wrap:wrap; gap:5px; }
  .tag-chip { background:#08120f; color:#99f6e4; border:1px solid #1b342c; padding:3px 9px; border-radius:999px; font-size:0.73rem; cursor:pointer; }
  .tag-chip.active { border-color:var(--glow); background:rgba(52,211,153,0.2); color:#ecfdf5; font-weight:700; }
  .pager {
    display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;
    background:#07100d; border:1px solid var(--border); padding:7px 11px; border-radius:10px; font-size:0.8rem;
  }
</style>
</head>
<body>

<header>
  <div class="brand">
    <svg width="38" height="38" viewBox="0 0 64 64" fill="none">
      <ellipse cx="32" cy="54" rx="16" ry="4" fill="rgba(52,211,153,0.22)"/>
      <path d="M25 32 C25 44, 23 50, 26 53 L38 53 C41 50, 39 44, 39 32 Z" fill="#d1fae5"/>
      <path d="M8 33 C8 16, 20 8, 32 8 C44 8, 56 16, 56 33 C47 36, 17 36, 8 33 Z" fill="url(#capGrad)" stroke="#6ee7b7" stroke-width="2"/>
      <circle cx="22" cy="21" r="4" fill="#fef08a" opacity="0.9"/>
      <circle cx="38" cy="17" r="3" fill="#fef08a" opacity="0.85"/>
      <circle cx="44" cy="26" r="2.5" fill="#a7f3d0" opacity="0.9"/>
      <defs>
        <linearGradient id="capGrad" x1="8" y1="8" x2="56" y2="34" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stop-color="#10b981"/>
          <stop offset="100%" stop-color="#0d9488"/>
        </linearGradient>
      </defs>
    </svg>
    <div>
      <h1>SporeVault</h1>
      <div class="sub">Bioluminescent Mycelium Memory · Paired with SporeFlow :9090</div>
    </div>
  </div>
  <div class="pills">
    <span class="pill">🔒 ID 鎖定</span>
    <span class="pill gold">⏱️ 強制 ms</span>
    <span class="pill">🟢 127.0.0.1:8081</span>
  </div>
</header>

<!-- 1. 雙模式查詢：:9090 孢子向量搜尋 + 小 SELECT -->
<div class="card">
  <div class="row">
    <input type="text" id="qInput" placeholder="🍄 向量搜尋（支援 #hashtag，例：#prot_read 唯讀鎖定）..." onkeydown="if(event.key==='Enter') runSearch()">
    <button onclick="runSearch()">⚡ :9090 搜尋</button>
    <button class="sec" onclick="clearResults()">清除</button>
  </div>
  <div class="row">
    <input type="text" id="selInput" placeholder="🔎 小 SELECT（例：SELECT * WHERE cluster=docs tag=#kvm LIMIT 10）" onkeydown="if(event.key==='Enter') runMiniSelect()">
    <button class="sec" style="border-color:#2dd4bf;color:#99f6e4;" onclick="runMiniSelect()">▶ 執行 SELECT</button>
  </div>
  <div class="row" style="gap:6px;">
    <span style="font-size:0.73rem;color:var(--muted);">快選範本：</span>
    <button class="chip-btn" onclick="quickSelect('SELECT * WHERE cluster=docs LIMIT 10')">cluster=docs</button>
    <button class="chip-btn" onclick="quickSelect('SELECT * WHERE tag=#prot_read LIMIT 10')">tag=#prot_read</button>
    <button class="chip-btn" onclick="quickSelect('SELECT * WHERE id>=500 cluster=rust-code LIMIT 10')">id>=500 rust-code</button>
    <button class="chip-btn" onclick="quickSelect('SELECT * WHERE tag=#wasm LIMIT 10')">tag=#wasm</button>
  </div>
  <div id="searchTime" class="timebar">⏱️ 進出時間：待機中（強制單位：ms）</div>
  <div id="searchResults" style="display:flex;flex-direction:column;gap:8px;"></div>
</div>

<!-- 2. 種下新孢子筆記 -->
<div class="card">
  <div class="row">
    <select id="addCluster">
      <option value="notes">🍄 叢集：notes</option>
      <option value="rust-code">🍄 叢集：rust-code</option>
      <option value="docs">🍄 叢集：docs</option>
      <option value="config">🍄 叢集：config</option>
    </select>
    <input type="text" id="addInput" placeholder="種下一筆新記憶（自動鎖定順序 ID 並抽取 #hashtag，例：測試完成 #bazzite #rust）" onkeydown="if(event.key==='Enter') addNote()">
    <button onclick="addNote()">🌱 存入並同步</button>
  </div>
  <div id="addStatus" style="font-size:0.76rem;color:var(--muted);"></div>
</div>

<!-- 3. 左側菌絲分區 / 右側真分頁檢視 -->
<div class="grid">
  <div class="card" style="gap:14px;">
    <div>
      <div class="section-title" style="color:#e9d5ff;margin-bottom:7px;">🍄 菌絲叢集 (Clusters)</div>
      <div id="clusterList"></div>
    </div>
    <div>
      <div class="section-title" style="color:#99f6e4;margin-bottom:7px;">🚩 孢子旗幟 (#Hashtags)</div>
      <div id="tagList" class="tag-cloud"></div>
    </div>
    <div>
      <div class="section-title" style="margin-bottom:7px;">📁 來源檔案</div>
      <div id="fileList"></div>
    </div>
  </div>

  <div class="card">
    <div class="row" style="justify-content:space-between;">
      <div class="section-title">📜 記憶分頁瀏覽 <span id="filterSummary" style="color:var(--muted);font-weight:400;"></span></div>
      <div class="row">
        <input type="text" id="kwFilter" placeholder="頁內過濾..." style="max-width:175px;padding:5px 9px;" oninput="resetPageAndLoad()">
        <select id="pageSizeSel" onchange="resetPageAndLoad()" style="padding:5px 8px;">
          <option value="20" selected>20 筆 / 頁</option>
          <option value="50">50 筆 / 頁</option>
          <option value="10">10 筆 / 頁</option>
        </select>
        <button class="sec" style="padding:5px 10px;" onclick="resetAllFilters()">重設</button>
      </div>
    </div>

    <div id="browseTime" class="timebar">⏱️ 分頁讀取：-- ms</div>

    <div class="pager">
      <button class="sec" id="prevBtn" onclick="changePage(-1)">◀ 上一頁</button>
      <span id="pageInfo">第 1 / 1 頁</span>
      <button class="sec" id="nextBtn" onclick="changePage(1)">下一頁 ▶</button>
    </div>

    <div id="recordList" style="display:flex;flex-direction:column;gap:8px;"></div>
  </div>
</div>

<script>
let activeCluster = '';
let activeTag = '';
let activeFile = '';
let currentPage = 1;
let totalPages = 1;

function quickSelect(expr) {
  document.getElementById('selInput').value = expr;
  runMiniSelect();
}

async function runMiniSelect() {
  const expr = document.getElementById('selInput').value.trim();
  if (!expr) return;
  const r = await fetch('/api/select?expr=' + encodeURIComponent(expr));
  const d = await r.json();
  const tb = document.getElementById('searchTime');
  const box = document.getElementById('searchResults');
  tb.innerText = `⏱️ [小 SELECT 進出時間] 進 ${d.in_str} ➔ 出 ${d.out_str} | 耗時 ${d.elapsed_ms.toFixed(3)} ms（命中 ${d.matched_total} 筆，回傳 ${d.returned} 筆）`;
  if (!d.records || d.records.length === 0) {
    box.innerHTML = '<div style="color:var(--muted);font-size:0.82rem;">查無符合 SELECT 條件之紀錄</div>';
    return;
  }
  box.innerHTML = d.records.map(rec => `
    <div class="item" style="border-color:#2dd4bf;">
      <div class="meta">
        <span>🔒#${rec.id} <span class="badge-cl">🍄 ${esc(rec.cluster)}</span> · 📄 ${esc(rec.source)}</span>
        <span class="ts">存入：${rec.created_str}</span>
      </div>
      <div class="row" style="gap:5px;">${(rec.tags||[]).map(t => `<span class="badge-tag" onclick="selectTag('${esc(t)}')">${esc(t)}</span>`).join('')}</div>
      <pre>${esc(rec.text)}</pre>
    </div>
  `).join('');
}

async function runSearch() {
  const q = document.getElementById('qInput').value.trim();
  if (!q) return;
  const url = `/api/search?q=${encodeURIComponent(q)}&cluster=${encodeURIComponent(activeCluster)}&tag=${encodeURIComponent(activeTag)}`;
  const r = await fetch(url);
  const d = await r.json();
  const tb = document.getElementById('searchTime');
  const box = document.getElementById('searchResults');
  if (d.error) { tb.innerText = '❌ ' + d.error; return; }
  const scope = [d.applied_cluster ? `叢集:${d.applied_cluster}` : '', d.applied_tag ? `旗幟:${d.applied_tag}` : ''].filter(Boolean).join(' · ') || '全庫';
  tb.innerText = `⏱️ [:9090 進出時間 | ${scope}] 進 ${d.in_str} ➔ 出 ${d.out_str} | 總往返 ${d.elapsed_ms.toFixed(3)} ms（核心 ${d.kernel_ms.toFixed(3)} ms · 歸零 ${d.zero_ms.toFixed(3)} ms）`;
  if (!d.hits || d.hits.length === 0) {
    box.innerHTML = '<div style="color:var(--muted);font-size:0.82rem;">無符合條件之高相關片段</div>';
    return;
  }
  box.innerHTML = d.hits.map((h, i) => `
    <div class="item" style="border-color:#34d399;">
      <div class="meta">
        <span><strong>[Top-${i+1}]</strong> 🔒#${h.id} <span class="badge-cl">🍄 ${esc(h.cluster)}</span> · 分數 ${h.score.toFixed(4)} · 📄 ${esc(h.source)}</span>
        <span class="ts">存入：${h.created_str}</span>
      </div>
      <div class="row" style="gap:5px;">${(h.tags||[]).map(t => `<span class="badge-tag" onclick="selectTag('${esc(t)}')">${esc(t)}</span>`).join('')}</div>
      <pre>${esc(h.text)}</pre>
    </div>
  `).join('');
}

function clearResults() {
  document.getElementById('qInput').value = '';
  document.getElementById('selInput').value = '';
  document.getElementById('searchResults').innerHTML = '';
  document.getElementById('searchTime').innerText = '⏱️ 進出時間：待機中（強制單位：ms）';
}

async function addNote() {
  const inp = document.getElementById('addInput');
  const cl = document.getElementById('addCluster').value;
  const txt = inp.value.trim();
  if (!txt) return;
  const r = await fetch(`/api/add?cluster=${encodeURIComponent(cl)}`, { method:'POST', body: txt });
  const d = await r.json();
  if (d.error) { document.getElementById('addStatus').innerText = '❌ ' + d.error; return; }
  inp.value = '';
  document.getElementById('addStatus').innerText = `🌱 已鎖定 ID #${d.id} [${d.cluster}] ${(d.tags||[]).join(' ')} | 進 ${d.in_str} ➔ 出 ${d.out_str} | 總 ${d.elapsed_ms.toFixed(3)} ms（同步 ${d.sync_ms.toFixed(3)} ms）`;
  loadRecords();
}

async function loadRecords() {
  const kw = document.getElementById('kwFilter').value.trim();
  const ps = document.getElementById('pageSizeSel').value;
  const url = `/api/records?cluster=${encodeURIComponent(activeCluster)}&tag=${encodeURIComponent(activeTag)}&file=${encodeURIComponent(activeFile)}&kw=${encodeURIComponent(kw)}&page=${currentPage}&page_size=${ps}`;
  const r = await fetch(url);
  const d = await r.json();

  currentPage = d.page;
  totalPages = d.total_pages;
  document.getElementById('filterSummary').innerText = `(${d.total_filtered} / ${d.total} 筆)`;
  document.getElementById('browseTime').innerText = `⏱️ [分頁進出] 進 ${d.in_str} ➔ 出 ${d.out_str} | ${d.elapsed_ms.toFixed(3)} ms`;
  document.getElementById('pageInfo').innerText = `第 ${d.page} / ${d.total_pages} 頁（符合 ${d.total_filtered} 筆，本頁 ${d.records.length} 筆）`;
  document.getElementById('prevBtn').disabled = (d.page <= 1);
  document.getElementById('nextBtn').disabled = (d.page >= d.total_pages);

  document.getElementById('clusterList').innerHTML =
    `<button class="side-btn ${activeCluster===''?'active':''}" onclick="selectCluster('')"><span>全部叢集</span><span>${d.total}</span></button>` +
    d.clusters.map(c => `<button class="side-btn ${activeCluster===c.name?'active':''}" onclick="selectCluster('${esc(c.name)}')"><span>🍄 ${esc(c.name)}</span><span>${c.count}</span></button>`).join('');

  document.getElementById('tagList').innerHTML =
    d.tags.map(t => `<span class="tag-chip ${activeTag===t.name?'active':''}" onclick="selectTag('${esc(t.name)}')">${esc(t.name)} (${t.count})</span>`).join('');

  document.getElementById('fileList').innerHTML =
    `<button class="side-btn ${activeFile===''?'active':''}" onclick="selectFile('')"><span>全部檔案</span><span>${d.total}</span></button>` +
    d.files.map(f => `<button class="side-btn ${activeFile===f.name?'active':''}" onclick="selectFile('${esc(f.name)}')"><span>${esc(f.name)}</span><span>${f.count}</span></button>`).join('');

  document.getElementById('recordList').innerHTML = d.records.map(rec => `
    <div class="item">
      <div class="meta">
        <span>🔒#${rec.id} <span class="badge-cl">🍄 ${esc(rec.cluster)}</span> · 📄 ${esc(rec.source)}</span>
        <span class="ts">存入：${rec.created_str}</span>
      </div>
      <div class="row" style="gap:5px;">${(rec.tags||[]).map(t => `<span class="badge-tag" onclick="selectTag('${esc(t)}')">${esc(t)}</span>`).join('')}</div>
      <pre>${esc(rec.text)}</pre>
    </div>
  `).join('');
}

function selectCluster(name) { activeCluster = (activeCluster === name ? '' : name); currentPage = 1; loadRecords(); }
function selectTag(name) { activeTag = (activeTag === name ? '' : name); currentPage = 1; loadRecords(); }
function selectFile(name) { activeFile = (activeFile === name ? '' : name); currentPage = 1; loadRecords(); }
function resetPageAndLoad() { currentPage = 1; loadRecords(); }
function resetAllFilters() { activeCluster = ''; activeTag = ''; activeFile = ''; document.getElementById('kwFilter').value = ''; currentPage = 1; loadRecords(); }
function changePage(delta) { currentPage = Math.max(1, Math.min(totalPages, currentPage + delta)); loadRecords(); }
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

loadRecords();
</script>
</body>
</html>
"##;
