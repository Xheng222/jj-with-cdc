use redb::TableDefinition;




// 常量
pub const MAX_BINARY_FILE_HEAD_SIZE: usize = 1024 * 8;
pub const LARGE_FILE_THRESHOLD: u64 = 1024 * 1024; // 1MB
pub const MAX_PACK_SIZE: u32 = 128 * 1024 * 1024; // 128MB
pub const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8MB
pub const CHUNK_AVG_SIZE: u32 = 16 * 1024; // 16KB

pub const HASH_LENGTH: usize = 32;

// 路径
pub const PACKS_DIR: &str = "packs";
pub const MANIFEST_GIT_DIR: &str = "manifest.git";
pub const GLOBAL_LOCK : &str = "global.lock";
pub const CHUNK_DB: &str = "chunk.redb";
pub const MANIFEST_ANCHOR_REF: &str = "refs/manifests/anchor";

// 数据表定义
pub const TABLE_CHUNKS: TableDefinition<[u8; HASH_LENGTH], [u8; 12]> = TableDefinition::new("chunks");

