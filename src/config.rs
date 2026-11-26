//! SporeVault 設定常數與直通協定規格。
//! 嚴格對齊 SporeFlow 的 128 維 f32 向量、:8080 原始二進位池灌入與 :9090 零 HTTP 直連埠。

use std::path::PathBuf;

/// 與 SporeFlow 一致的向量維度（128 維 f32 = 512 Bytes）
pub const VECTOR_DIM: usize = 128;

/// SporeFlow :9090 單次微批次查詢向量數（6 × 128 × 4 = 3,072 Bytes）
pub const MICRO_BATCH_SIZE: usize = 6;

/// 單筆向量位元組數（512 Bytes）
pub const VECTOR_BYTES: usize = VECTOR_DIM * 4;

/// :9090 請求封包大小（3,072 Bytes）與回應封包大小（56 Bytes）
pub const CONDUIT_REQ_BYTES: usize = MICRO_BATCH_SIZE * VECTOR_BYTES;
pub const CONDUIT_RESP_BYTES: usize = 56;

/// SporeFlow HTTP /v1/pool 單次接收上限為 512KB（扣除 HTTP Header 後安全上限為 980 筆向量）
pub const MAX_POOL_SYNC_VECTORS: usize = 980;

/// 預設切塊行數（匯入文字檔時每幾行切成一筆可檢索片段）
pub const DEFAULT_CHUNK_LINES: usize = 6;

/// SporeVault 單檔二進位資料庫魔數（v3 新增叢集 `cluster` 與 `#hashtag` 旗幟清單）
pub const VAULT_MAGIC_V1: &[u8; 8] = b"SPVDB001";
pub const VAULT_MAGIC_V2: &[u8; 8] = b"SPVDB002";
pub const VAULT_MAGIC_V3: &[u8; 8] = b"SPVDB003";

/// 預設 SporeFlow 本機直連位址與 SporeVault 簡易看內容網頁埠（僅綁定 127.0.0.1）
pub const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:8080";
pub const DEFAULT_BINARY_ADDR: &str = "127.0.0.1:9090";
pub const DEFAULT_UI_ADDR: &str = "127.0.0.1:8081";

/// 查詢最低有效相似度門檻（過濾無關鬆散雜訊）
pub const MIN_DISPLAY_SCORE: f32 = 0.22;

/// 預設每頁顯示筆數（防止一次全表吐出塞爆瀏覽器與查詢）
pub const DEFAULT_PAGE_SIZE: usize = 20;

/// 預設單檔資料庫路徑（~/.local/share/sporevault/vault.svdb）
pub fn default_vault_path() -> PathBuf {
    if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("sporevault")
            .join("vault.svdb")
    } else {
        PathBuf::from("vault.svdb")
    }
}
