//! SporeVault 核心引擎：
//! 1. 零外部依賴單檔二進位持久化（vault.svdb）
//! 2. 與 SporeFlow 一致的 FNV-1a N-gram 128D L2 正規化特徵投影
//! 3. 直接通路同步（POST /v1/pool 原始 Float32 二進位）與 :9090 長連線零解析查詢

use crate::config::{
    CONDUIT_REQ_BYTES, CONDUIT_RESP_BYTES, DEFAULT_CHUNK_LINES, MAX_POOL_SYNC_VECTORS,
    MICRO_BATCH_SIZE, VAULT_MAGIC_V1, VAULT_MAGIC_V2, VAULT_MAGIC_V3, VECTOR_BYTES, VECTOR_DIM,
};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// 取得當前強制統一時間單位：Unix Epoch 毫秒（ms）
pub fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 將 Unix 毫秒時間戳格式化為台灣時間（UTC+8）`YYYY-MM-DD HH:MM:SS.mmm`（零外部依賴）
pub fn format_taipei_ms(epoch_ms: u64) -> String {
    let local_ms = epoch_ms.saturating_add(8 * 3600 * 1000);
    let total_secs = local_ms / 1000;
    let ms = local_ms % 1000;
    let secs_of_day = total_secs % 86400;
    let hour = secs_of_day / 3600;
    let min = (secs_of_day % 3600) / 60;
    let sec = secs_of_day % 60;

    // Howard Hinnant's civil_from_days algorithm
    let z = (total_secs / 86400) as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}.{ms:03}")
}

/// 單筆持久化紀錄（寫入即鎖死遞增 ID 與毫秒時間戳，附帶叢集 `cluster` 與 `#hashtag` 旗幟）
#[derive(Debug, Clone)]
pub struct VaultRecord {
    pub id: u32,
    pub created_at_ms: u64,
    pub cluster: String,
    pub tags: Vec<String>,
    pub source: String,
    pub text: String,
    pub vector: [f32; VECTOR_DIM],
}

/// :9090 直連微批次查詢回傳結果（強制附帶進出毫秒時間戳 `in_ms` / `out_ms` 與毫秒耗時）
#[derive(Debug, Clone)]
pub struct ConduitBatchHit {
    pub ids: [u32; MICRO_BATCH_SIZE],
    pub scores: [f32; MICRO_BATCH_SIZE],
    pub in_epoch_ms: u64,
    pub out_epoch_ms: u64,
    pub kernel_compute_ms: f64,
    pub kernel_zero_ms: f64,
    pub roundtrip_ms: f64,
}

/// 迷你向量資料庫本體（落盤單檔 + 記憶體 O(1) ID 對照）
pub struct SporeVault {
    pub records: Vec<VaultRecord>,
}

impl SporeVault {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// 從單檔二進位 vault.svdb 秒速載入（相容 v1 / v2 / v3 叢集與旗幟格式）
    pub fn load(path: &Path) -> io::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let mut f = File::open(path)?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        let ver = if &magic == VAULT_MAGIC_V3 {
            3u8
        } else if &magic == VAULT_MAGIC_V2 {
            2u8
        } else if &magic == VAULT_MAGIC_V1 {
            1u8
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "無效的 vault.svdb 檔案格式",
            ));
        };

        let count = read_u32(&mut f)? as usize;
        let mut records = Vec::with_capacity(count);
        let fallback_ms = now_epoch_ms();
        for _ in 0..count {
            let id = read_u32(&mut f)?;
            let created_at_ms = if ver >= 2 { read_u64(&mut f)? } else { fallback_ms };

            let (cluster, tags) = if ver >= 3 {
                let clen = read_u32(&mut f)? as usize;
                let mut cb = vec![0u8; clen];
                f.read_exact(&mut cb)?;
                let cstr = String::from_utf8_lossy(&cb).into_owned();
                let tcnt = read_u32(&mut f)? as usize;
                let mut tvec = Vec::with_capacity(tcnt);
                for _ in 0..tcnt {
                    let tl = read_u32(&mut f)? as usize;
                    let mut tb = vec![0u8; tl];
                    f.read_exact(&mut tb)?;
                    tvec.push(String::from_utf8_lossy(&tb).into_owned());
                }
                (cstr, tvec)
            } else {
                (String::new(), Vec::new())
            };

            let src_len = read_u32(&mut f)? as usize;
            let mut src_bytes = vec![0u8; src_len];
            f.read_exact(&mut src_bytes)?;
            let source = String::from_utf8_lossy(&src_bytes).into_owned();

            let txt_len = read_u32(&mut f)? as usize;
            let mut txt_bytes = vec![0u8; txt_len];
            f.read_exact(&mut txt_bytes)?;
            let text = String::from_utf8_lossy(&txt_bytes).into_owned();

            let mut vec_raw = [0u8; VECTOR_BYTES];
            f.read_exact(&mut vec_raw)?;
            let mut vector = [0.0f32; VECTOR_DIM];
            for (d, slot) in vector.iter_mut().enumerate() {
                let off = d * 4;
                *slot = f32::from_le_bytes([
                    vec_raw[off],
                    vec_raw[off + 1],
                    vec_raw[off + 2],
                    vec_raw[off + 3],
                ]);
            }

            let final_cluster = if cluster.is_empty() {
                infer_cluster(&source)
            } else {
                cluster
            };
            let final_tags = if tags.is_empty() {
                extract_hashtags(&source, &text)
            } else {
                tags
            };

            records.push(VaultRecord {
                id,
                created_at_ms,
                cluster: final_cluster,
                tags: final_tags,
                source,
                text,
                vector,
            });
        }
        Ok(Self { records })
    }

    /// 原子寫入單檔二進位 vault.svdb（v3 格式：鎖死 ID + 強制 ms 時間戳 + 叢集 + #hashtag 旗幟）
    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = path.with_extension("svdb.tmp");
        {
            let mut f = File::create(&tmp_path)?;
            f.write_all(VAULT_MAGIC_V3)?;
            f.write_all(&(self.records.len() as u32).to_le_bytes())?;
            for rec in &self.records {
                f.write_all(&rec.id.to_le_bytes())?;
                f.write_all(&rec.created_at_ms.to_le_bytes())?;
                let cb = rec.cluster.as_bytes();
                f.write_all(&(cb.len() as u32).to_le_bytes())?;
                f.write_all(cb)?;
                f.write_all(&(rec.tags.len() as u32).to_le_bytes())?;
                for t in &rec.tags {
                    let tbytes = t.as_bytes();
                    f.write_all(&(tbytes.len() as u32).to_le_bytes())?;
                    f.write_all(tbytes)?;
                }
                let sb = rec.source.as_bytes();
                f.write_all(&(sb.len() as u32).to_le_bytes())?;
                f.write_all(sb)?;
                let tb = rec.text.as_bytes();
                f.write_all(&(tb.len() as u32).to_le_bytes())?;
                f.write_all(tb)?;
                for &val in &rec.vector {
                    f.write_all(&val.to_le_bytes())?;
                }
            }
            f.flush()?;
        }
        fs::rename(tmp_path, path)?;
        Ok(())
    }

    /// 新增單筆文字片段（鎖死遞增 ID、打上 ms 時間戳、自動分群 cluster 與抽取 #hashtag 旗幟）
    pub fn add_text(&mut self, source: &str, text: &str) -> u32 {
        self.add_text_with_cluster(source, text, None)
    }

    pub fn add_text_with_cluster(
        &mut self,
        source: &str,
        text: &str,
        cluster_override: Option<&str>,
    ) -> u32 {
        let trimmed = text.trim();
        let id = self.records.len() as u32;
        let cluster = cluster_override
            .filter(|c| !c.trim().is_empty())
            .map(|c| c.trim().to_string())
            .unwrap_or_else(|| infer_cluster(source));
        let tags = extract_hashtags(source, trimmed);
        let vector = encode_text_to_128d(trimmed);
        self.records.push(VaultRecord {
            id,
            created_at_ms: now_epoch_ms(),
            cluster,
            tags,
            source: source.to_string(),
            text: trimmed.to_string(),
            vector,
        });
        id
    }

    /// 讀取單一文字檔或整個目錄並自動切塊寫入（回傳新增筆數）
    pub fn ingest_path(&mut self, target: &Path) -> io::Result<usize> {
        let mut added = 0usize;
        if target.is_file() {
            added += self.ingest_single_file(target)?;
        } else if target.is_dir() {
            for entry in fs::read_dir(target)? {
                let entry = entry?;
                let p = entry.path();
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') || name == "target" || name == "node_modules" {
                    continue;
                }
                if p.is_file() && is_text_like_file(&p) {
                    added += self.ingest_single_file(&p)?;
                } else if p.is_dir() {
                    added += self.ingest_path(&p)?;
                }
            }
        }
        Ok(added)
    }

    fn ingest_single_file(&mut self, file_path: &Path) -> io::Result<usize> {
        let content = match fs::read_to_string(file_path) {
            Ok(c) => c,
            Err(_) => return Ok(0), // 略過非 UTF-8 二進位檔
        };
        let src_label = file_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| file_path.display().to_string());

        let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.is_empty() {
            return Ok(0);
        }

        let mut added = 0usize;
        for (chunk_idx, group) in lines.chunks(DEFAULT_CHUNK_LINES).enumerate() {
            if self.records.len() >= MAX_POOL_SYNC_VECTORS {
                break;
            }
            let chunk_text = group.join("\n");
            let label = format!("{src_label}#chunk{}", chunk_idx + 1);
            self.add_text(&label, &chunk_text);
            added += 1;
        }
        Ok(added)
    }

    /// 將本地資料庫的向量以原始 Float32 二進位直接同步灌入 SporeFlow `/v1/pool`（鎖死 PROT_READ）
    pub fn sync_to_sporeflow(&self, http_addr: &str) -> io::Result<(usize, f64, String)> {
        if self.records.is_empty() {
            return Ok((0, 0.0, "empty vault".to_string()));
        }
        let sync_count = self.records.len().min(MAX_POOL_SYNC_VECTORS);
        let mut raw_bytes = Vec::with_capacity(sync_count * VECTOR_BYTES);
        for rec in self.records.iter().take(sync_count) {
            for &v in &rec.vector {
                raw_bytes.extend_from_slice(&v.to_le_bytes());
            }
        }

        let t0 = Instant::now();
        let mut sock = TcpStream::connect(http_addr)?;
        let _ = sock.set_nodelay(true);
        let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));

        let header = format!(
            "POST /v1/pool HTTP/1.1\r\nHost: {http_addr}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            raw_bytes.len()
        );
        sock.write_all(header.as_bytes())?;
        sock.write_all(&raw_bytes)?;
        sock.flush()?;

        let mut resp = Vec::new();
        sock.read_to_end(&mut resp)?;
        let elapsed_us = t0.elapsed().as_secs_f64() * 1_000_000.0;
        let resp_str = String::from_utf8_lossy(&resp);
        let body = resp_str.lines().last().unwrap_or("").to_string();
        Ok((sync_count, elapsed_us, body))
    }

    /// O(1) 依 ID 取出原文紀錄
    pub fn get(&self, id: u32) -> Option<&VaultRecord> {
        self.records.get(id as usize)
    }
}

/// 維持單條 :9090 長連線的高速純二進位直通客戶端（零 HTTP 開銷、零重複握手）
pub struct DirectConduitClient {
    sock: TcpStream,
}

impl DirectConduitClient {
    pub fn connect(bin_addr: &str) -> io::Result<Self> {
        let sock = TcpStream::connect(bin_addr)?;
        sock.set_nodelay(true)?;
        sock.set_read_timeout(Some(Duration::from_secs(5)))?;
        Ok(Self { sock })
    }

    /// 將查詢拆成「完整句 + 多個關鍵子詞」塞滿 :9090 的 6 槽矩陣流，同一次往返直接收回最多 6 個不重複候選！
    pub fn query_text_topk(&mut self, query: &str) -> io::Result<ConduitBatchHit> {
        let sub_queries = expand_query_to_6_slots(query);
        let mut req_buf = [0u8; CONDUIT_REQ_BYTES];
        for (b, sq) in sub_queries.iter().enumerate() {
            let qvec = encode_text_to_128d(sq);
            for (d, &val) in qvec.iter().enumerate() {
                let off = (b * VECTOR_DIM + d) * 4;
                req_buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
            }
        }

        let in_epoch_ms = now_epoch_ms();
        let t0 = Instant::now();
        self.sock.write_all(&req_buf)?;
        self.sock.flush()?;
        let mut resp_buf = [0u8; CONDUIT_RESP_BYTES];
        self.sock.read_exact(&mut resp_buf)?;
        let roundtrip_ms = t0.elapsed().as_secs_f64() * 1_000.0;
        let out_epoch_ms = now_epoch_ms();

        let mut ids = [0u32; MICRO_BATCH_SIZE];
        let mut scores = [0.0f32; MICRO_BATCH_SIZE];
        for b in 0..MICRO_BATCH_SIZE {
            ids[b] = u32::from_le_bytes([
                resp_buf[b * 4],
                resp_buf[b * 4 + 1],
                resp_buf[b * 4 + 2],
                resp_buf[b * 4 + 3],
            ]);
            scores[b] = f32::from_le_bytes([
                resp_buf[24 + b * 4],
                resp_buf[24 + b * 4 + 1],
                resp_buf[24 + b * 4 + 2],
                resp_buf[24 + b * 4 + 3],
            ]);
        }
        let kernel_compute_ms =
            f32::from_le_bytes([resp_buf[48], resp_buf[49], resp_buf[50], resp_buf[51]]) as f64
                / 1_000.0;
        let kernel_zero_ms =
            f32::from_le_bytes([resp_buf[52], resp_buf[53], resp_buf[54], resp_buf[55]]) as f64
                / 1_000.0;

        Ok(ConduitBatchHit {
            ids,
            scores,
            in_epoch_ms,
            out_epoch_ms,
            kernel_compute_ms,
            kernel_zero_ms,
            roundtrip_ms,
        })
    }
}

/// 將單句查詢智慧展開為 6 個矩陣槽位（Slot 0 = 完整查詢，Slot 1..5 = 個別關鍵詞或重疊滑動視窗）
fn expand_query_to_6_slots(query: &str) -> [String; MICRO_BATCH_SIZE] {
    let trimmed = query.trim();
    let mut slots = [
        trimmed.to_string(),
        trimmed.to_string(),
        trimmed.to_string(),
        trimmed.to_string(),
        trimmed.to_string(),
        trimmed.to_string(),
    ];

    let words: Vec<&str> = trimmed
        .split(|c: char| c.is_whitespace() || c == ',' || c == '、' || c == '/')
        .filter(|w| !w.is_empty())
        .collect();

    if words.len() >= 2 {
        for (i, w) in words.iter().take(MICRO_BATCH_SIZE - 1).enumerate() {
            slots[i + 1] = (*w).to_string();
        }
        // 若詞數 < 5，用相鄰雙詞組合補齊剩餘槽位
        let mut next_slot = words.len() + 1;
        for pair in words.windows(2) {
            if next_slot >= MICRO_BATCH_SIZE {
                break;
            }
            slots[next_slot] = format!("{} {}", pair[0], pair[1]);
            next_slot += 1;
        }
    } else {
        // 單一長字串（例如中文長句或長函式名）：依字元切出重疊視窗填滿其餘槽位
        let chars: Vec<char> = trimmed.chars().collect();
        if chars.len() >= 4 {
            let win = (chars.len() / 2).max(2);
            for s in 1..MICRO_BATCH_SIZE {
                let start = (s * (chars.len() - win)) / (MICRO_BATCH_SIZE - 1);
                slots[s] = chars[start..start + win].iter().collect();
            }
        }
    }
    slots
}

/// 輕量零依賴文字特徵編碼（UTF-8 字元 2-gram / 3-gram + 單詞 FNV-1a 投影至 128 維並做 L2 正規化）
/// 同時支援英文單詞與中文詞組片段，讓字面／子字串檢索命中率更高
pub fn encode_text_to_128d(text: &str) -> [f32; VECTOR_DIM] {
    let mut vec = [0.0f32; VECTOR_DIM];
    let lower = text.to_lowercase();
    let chars: Vec<char> = lower.chars().filter(|c| !c.is_whitespace()).collect();
    if chars.is_empty() {
        vec[0] = 1.0;
        return vec;
    }

    // 1. 字元 2-gram（對中文與英文子字串特別有效）
    for win in chars.windows(2) {
        let mut h: u64 = 0xcbf29ce484222325;
        for &c in win {
            h ^= c as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let idx = (h as usize) % VECTOR_DIM;
        let sign = if (h >> 63) == 0 { 1.0 } else { -1.0 };
        vec[idx] += sign;
    }

    // 2. 字元 3-gram（強化精確片語權重）
    for win in chars.windows(3) {
        let mut h: u64 = 0x84222325cbf29ce4;
        for &c in win {
            h ^= c as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let idx = (h as usize) % VECTOR_DIM;
        let sign = if (h >> 62) & 1 == 0 { 1.5 } else { -1.5 };
        vec[idx] += sign;
    }

    // 3. 空白分隔完整詞（強化英文關鍵字與程式碼識別字）
    for token in lower.split(|c: char| !c.is_alphanumeric() && c != '_') {
        if token.len() < 2 {
            continue;
        }
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in token.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let idx = (h as usize) % VECTOR_DIM;
        let sign = if (h >> 61) & 1 == 0 { 2.0 } else { -2.0 };
        vec[idx] += sign;
    }

    // 4. 針對 #hashtag 旗幟額外加權（強化插旗檢索精準度）
    for tag in parse_inline_hashtags(text) {
        let mut h: u64 = 0x517cc1b727220a95;
        for &b in tag.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let idx = (h as usize) % VECTOR_DIM;
        let sign = if (h >> 60) & 1 == 0 { 3.0 } else { -3.0 };
        vec[idx] += sign;
    }

    // L2 正規化（長度歸 1，使點積直接等於餘弦相似度）
    let sum_sq: f32 = vec.iter().map(|x| x * x).sum();
    if sum_sq > 1e-12 {
        let inv = 1.0 / sum_sq.sqrt();
        for x in &mut vec {
            *x *= inv;
        }
    } else {
        vec[0] = 1.0;
    }
    vec
}

/// 依來源名稱自動歸類至叢集（Cluster，像 BigQuery 分區鍵）
pub fn infer_cluster(source: &str) -> String {
    let fname = source.split('#').next().unwrap_or(source).to_ascii_lowercase();
    if fname.ends_with(".rs") {
        "rust-code".to_string()
    } else if fname.ends_with(".md") || fname.ends_with(".txt") {
        "docs".to_string()
    } else if fname.ends_with(".yaml")
        || fname.ends_with(".yml")
        || fname.ends_with(".toml")
        || fname.ends_with(".container")
        || fname.ends_with(".json")
    {
        "config".to_string()
    } else if fname.contains("note") {
        "notes".to_string()
    } else {
        "general".to_string()
    }
}

/// 從內文擷取所有 `#hashtag`（支援中英文、數字、底線、連字號；自動排除 Rust `#[` 與 Markdown `##`）
pub fn parse_inline_hashtags(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '#' {
            let prev_ok = i == 0 || chars[i - 1].is_whitespace() || chars[i - 1] == '(';
            if prev_ok && i + 1 < chars.len() {
                let next = chars[i + 1];
                if next.is_alphabetic() || next > '\u{007F}' {
                    let mut j = i + 1;
                    while j < chars.len()
                        && (chars[j].is_alphanumeric()
                            || chars[j] == '_'
                            || chars[j] == '-'
                            || chars[j] > '\u{007F}')
                    {
                        j += 1;
                    }
                    let tag: String = chars[i..j].iter().collect::<String>().to_lowercase();
                    // 排除 6 位或 3 位純十六進位 CSS 色碼（如 #f59e0b、#e2e8f0）
                    let body = &tag[1..];
                    let is_hex_color = (body.len() == 6 || body.len() == 3)
                        && body.chars().all(|c| c.is_ascii_hexdigit())
                        && body.chars().any(|c| c.is_ascii_digit());
                    if tag.len() >= 2 && !is_hex_color && !out.contains(&tag) {
                        out.push(tag);
                    }
                    i = j;
                    continue;
                }
            }
        }
        i += 1;
    }
    out
}

/// 結合「內文明確 `#hashtag`」＋「系統自動關鍵領域插旗」，強化叢集內快速過濾
pub fn extract_hashtags(source: &str, text: &str) -> Vec<String> {
    let mut tags = parse_inline_hashtags(text);
    let lower = text.to_ascii_lowercase();
    let auto_rules: &[(&str, &str)] = &[
        ("mprotect", "#prot_read"),
        ("prot_read", "#prot_read"),
        ("wasm", "#wasm"),
        ("kvm", "#kvm"),
        ("9090", "#port9090"),
        ("docker", "#docker"),
        ("podman", "#podman"),
        ("hugepage", "#hugepages"),
        ("pheromone", "#routing"),
    ];
    for &(needle, flag) in auto_rules {
        if lower.contains(needle) && !tags.iter().any(|t| t == flag) {
            tags.push(flag.to_string());
        }
    }
    if source.contains("note") && !tags.iter().any(|t| t == "#note") {
        tags.push("#note".to_string());
    }
    tags
}

fn read_u32(r: &mut impl Read) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64(r: &mut impl Read) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn is_text_like_file(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "txt" | "md" | "rs" | "py" | "js" | "ts" | "json" | "yaml" | "yml" | "toml" | "log" | "sh" | "container"
    )
}
