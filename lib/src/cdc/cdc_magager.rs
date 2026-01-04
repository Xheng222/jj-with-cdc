#![expect(missing_docs)]








use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{PathBuf};

use bitcode::{Decode, Encode};
use digest::Digest;
use digest::consts::U32;
use fastcdc::v2020::FastCDC;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::debug;

use crate::cdc::pointer::CdcPointer;
use crate::hex_util;

// 常量
const MAX_BINARY_FILE_HEAD_SIZE: usize = 1024 * 8;
const LARGE_FILE_THRESHOLD: u64 = 1024 * 1024; // 1MB
const SUPER_CHUNK_SIZE: usize = 512 * 1024 * 1024; // 512MB 分段大小
const MAX_PACK_SIZE: usize = 128 * 1024 * 1024; // 128MB
const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8MB
const CHUNK_AVG_SIZE: u32 = 16 * 1024; // 16KB

// 路径
const PACKS_DIR: &str = "objects";
const GLOBLE_LOCK_PATH : &str = "global.lock";
const CHUNK_DB_PATH: &str = "chunk.redb";
const MANIFEST_DB_PATH: &str = "manifest.redb";

// 数据表定义
const TABLE_CHUNKS: TableDefinition<String, &[u8]> = TableDefinition::new("chunks");
const TABLE_MANIFESTS: TableDefinition<String, &[u8]> = TableDefinition::new("manifests");


#[derive(Encode, Decode, Debug)]
struct GlobalState {
    pack_id: String,
    current_size: usize,
}

impl GlobalState {
    fn open_file_writer(&self, pack_dir: &PathBuf) -> Result<BufWriter<File>, std::io::Error>{
        let pack_path = pack_dir.join(&self.pack_id);
        let file_writer = BufWriter::with_capacity(
            BUFFER_SIZE,
            std::fs::OpenOptions::new().create(true).write(true).append(true).open(&pack_path)?
        );

        Ok(file_writer)
    }
}

impl Default for GlobalState {
    fn default() -> Self {
        let pack_id = format!("{}.bin", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or(std::time::Duration::new(0,0)).as_nanos());
        GlobalState {
            pack_id: pack_id,
            current_size: 0,
        }
    }
}

/// 数据库内部存储的物理位置
#[derive(Encode, Decode, Debug)]
struct ChunkLocation<'a> {
    pack_id: &'a str, // 属于哪个 pack 文件
    offset: u64,     // 偏移量
    length: usize,     // 长度
}

/// 一个文件对应的所有块的 hash
#[derive(Encode, Decode, Debug)]
struct CdcManifest {
    chunks: Vec<String>,
}

// 内存中临时的 Chunk 数据包
struct PendingCdcChunk<'a> {
    hash: String,
    data: &'a [u8],
}

/// CDC manager
#[derive(Debug)]
pub struct CdcMagager {
    /// The path to the CDC store
    store_path: PathBuf,
    chunk_db: Option<Database>,
    manifest_db: Option<Database>,
    global_lock: Option<File>,
    global_state: Option<GlobalState>,
    pack_dir: Option<PathBuf>,
}

impl Drop for CdcMagager {
    fn drop(&mut self) {
        if let Some(chunk_db) = self.chunk_db.as_mut() {
            let _r = chunk_db.compact();
        }
        if let Some(manifest_db) = self.manifest_db.as_mut() {
            let _r = manifest_db.compact();
        }
    }
    
}

impl CdcMagager {
    pub fn new(store_path: PathBuf) -> Self {
        Self { 
            store_path: store_path, 
            chunk_db: None, 
            manifest_db: None,
            global_lock: None,
            global_state: None,
            pack_dir: None,
        }
    }

    /// Simple binary detection heuristic
    pub fn is_binary_file(file: &mut File) -> bool {
        // 如果大于 1M，直接视为二进制
        if let Ok(meta) = file.metadata() {
            if meta.len() > LARGE_FILE_THRESHOLD {
                return true;
            }
        }
    
        // 读取头部
        let mut buffer = [0u8; MAX_BINARY_FILE_HEAD_SIZE];
        let n = match file.read(&mut buffer) {
            Ok(n) => n,
            Err(_) => return false,
        };
        // 如果读取失败，则认为是二进制文件
        if n == 0 { return true; }

        // 重置文件指针到开头
        match file.seek(std::io::SeekFrom::Start(0)) {
            Ok(_) => (),
            Err(_) => return true,
        }
    
        let buffer = &buffer[..n];
    
        // - 检查 NULL 字节
        if buffer.contains(&0) {
            return true;
        }
    
        // - 检查 UTF-8 编码
        if String::from_utf8(buffer.to_vec()).is_ok() {
            return false;
        }
    
        // - 检查 UTF-16 LE 或 UTF-16 BE 编码
        if n > 2 && (buffer.starts_with(&[0xFF, 0xFE]) || buffer.starts_with(&[0xFE, 0xFF])) {
            return false;
        }
    
        // - 统计可打印 ASCII 字符
        // let non_ascii_count = buffer.iter().filter(|&b| *b > 127).count();
        let text_ascii_count = buffer.iter().filter(|&b| {
            (*b >= 0x20 && *b <= 0x7E) || 
            *b == 0x09 || *b == 0x0A || *b == 0x0D ||
            *b == 0x0C || *b == 0x08
        }).count();
    
    
        // 如果可打印 ASCII 字符占比超过 70%，则认为是二进制文件
        let text_ascii_ratio = text_ascii_count as f64 / n as f64;
        if text_ascii_ratio > 0.7 {
            return false;
        }
        return true;
    
    }

    fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.global_state.is_some() {
            return Ok(());
        }
        else {
            let pack_dir = self.store_path.join(PACKS_DIR);
            if !pack_dir.exists() {
                std::fs::create_dir_all(&pack_dir)?;
            }

            let chunk_db = Database::create(self.store_path.join(CHUNK_DB_PATH))?;
            let manifest_db = Database::create(self.store_path.join(MANIFEST_DB_PATH))?;


            let mut file = std::fs::OpenOptions::new().create(true).write(true).open(self.store_path.join(GLOBLE_LOCK_PATH))?;
            file.lock()?;
            let global_state: GlobalState = {
                let mut content = Vec::new();
                match file.read_to_end(&mut content) {
                    Ok(_) => bitcode::decode(&content).unwrap_or_default(),
                    Err(_) => GlobalState::default(),
                }
            };

            self.chunk_db = Some(chunk_db);
            self.manifest_db = Some(manifest_db);
            self.global_lock = Some(file);
            self.global_state = Some(global_state);
            self.pack_dir = Some(pack_dir);
            Ok(())
        }

    }

    fn flush_pending_chunks<'a>(
        &mut self,
        chunks: &Vec<(usize, usize)>,
        mmap: &[u8],
        chunk_hashes: &mut Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pending_chunks: Vec<PendingCdcChunk> = chunks.par_iter()
            .map(|&(offset, length)| {
                let hash = calculate_hash(&mmap[offset .. offset + length]);
                 PendingCdcChunk {
                    hash: hash,
                    data: &mmap[offset .. offset + length],
                }
            }).collect();

        for chunk in &pending_chunks {
            chunk_hashes.push(chunk.hash.clone());
        }
        
        let global_state = self.global_state.as_mut().unwrap();
        let pack_dir = self.pack_dir.as_ref().unwrap();
        let chunk_db = self.chunk_db.as_mut().unwrap();

        let chunk_wtx = chunk_db.begin_write()?;
        let mut pack_writer = global_state.open_file_writer(&pack_dir)?;
        {
            let mut chunk_table = chunk_wtx.open_table(TABLE_CHUNKS)?;
            for chunk in pending_chunks {
                if chunk_table.get(&chunk.hash)?.is_some() {
                    continue; // 已存在
                }
                // 写入 Pack
                pack_writer.write_all(chunk.data)?;
                
                // 记录索引
                let loc = ChunkLocation {
                    pack_id: &global_state.pack_id,
                    offset: global_state.current_size as u64,
                    length: chunk.data.len(),
                };
                let loc_bytes = bitcode::encode(&loc);
                chunk_table.insert(&chunk.hash, loc_bytes.as_slice())?;

                // 更新内存中的 State 计数
                global_state.current_size += chunk.data.len();
            }
        }
        chunk_wtx.commit()?;
        // 刷新 Pack 缓冲区
        pack_writer.flush()?;
    
        // 检查是否需要切换 Pack 文件
        if global_state.current_size >= MAX_PACK_SIZE {
            *global_state = GlobalState::default();
        }
    
        Ok(())
    }

    #[allow(unsafe_code)]
    pub fn write_file_to_cdc(&mut self, file: &File) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.init()?;

        let mmap = unsafe {
            memmap2::MmapOptions::new().map(file)?
        };

        let mut segments = Vec::new();
        let mut start = 0;
        while start < mmap.len() {
            let end = std::cmp::min(start + SUPER_CHUNK_SIZE, mmap.len());
            segments.push((start, end));
            start = end;
        }

        // 并行处理所有分段
        let all_chunks: Vec<(usize, usize)> = segments.par_iter().flat_map(|&(seg_start, seg_end)| {
            // 在每个分段内使用 FastCDC 切片
            let segment = &mmap[seg_start .. seg_end];
            let chunker = FastCDC::new(segment, CHUNK_AVG_SIZE / 4, CHUNK_AVG_SIZE, CHUNK_AVG_SIZE * 4);
            chunker.map(|chunk| (seg_start + chunk.offset, chunk.length)).collect::<Vec<(usize, usize)>>()
        }).collect();

        // 遍历切片
        let mut chunks: Vec<(usize, usize)> = Vec::new();
        let mut chunk_hashes = Vec::new();
        let mut chunks_length = 0usize;
        for chunk in all_chunks {
            chunks.push(chunk);
            chunks_length += chunk.1;
            if chunks_length >= MAX_PACK_SIZE / 2 {
                self.flush_pending_chunks(&chunks, &mmap, &mut chunk_hashes)?;
                chunks_length = 0;
                chunks.clear();
            }
        }

        // 刷新剩余的待写入块
        if !chunks.is_empty() {
            self.flush_pending_chunks(&chunks, &mmap, &mut chunk_hashes)?;
            chunks.clear();
        }

        // 存储 Manifest
        let manifest = CdcManifest {
            chunks: chunk_hashes,
        };

        let manifest_txn = self.chunk_db.as_mut().unwrap().begin_write()?;
        let manifest_encoded = bitcode::encode(&manifest);
        let manifest_hash = calculate_hash(&manifest_encoded);

        {
            let mut manifest_table = manifest_txn.open_table(TABLE_MANIFESTS)?;
            if manifest_table.get(&manifest_hash)?.is_none() {
                manifest_table.insert(&manifest_hash, manifest_encoded.as_slice())?;
            }
        }
        manifest_txn.commit()?;

        {
            // 写入 lock 文件
            let state_bytes = bitcode::encode(self.global_state.as_ref().unwrap());
            let global_lock = self.global_lock.as_mut().unwrap();
            global_lock.set_len(0)?; // 清空文件
            global_lock.seek(SeekFrom::Start(0))?;
            global_lock.write_all(&state_bytes)?;
            global_lock.flush()?;
        };

        // 生成一个 CDC 指针
        let pointer = CdcPointer::new(manifest_hash);
        let pointer_bytes = pointer.serialize();
        debug!("CDC Pointer: {:?}", pointer);
        Ok(pointer_bytes)
    }


    pub fn read_file_from_cdc(&mut self, pointer: &CdcPointer, file: &File) -> Result<usize, Box<dyn std::error::Error>> {
        self.init()?;
        let chunks = {
            let manifest_txn = self.chunk_db.as_mut().unwrap().begin_read()?;
            let manifest_table = manifest_txn.open_table(TABLE_MANIFESTS)?;

            let manifest_bytes = match manifest_table.get(pointer.hash())? {
                Some(bytes) => bytes,
                None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("无法找到对应的 Manifest 数据: {}", pointer.hash())))),
            };

            let manifest: CdcManifest = bitcode::decode(manifest_bytes.value())?;
            manifest.chunks
        };

        let read_txn = self.chunk_db.as_mut().unwrap().begin_read()?;
        let chunk_table = read_txn.open_table(TABLE_CHUNKS)?;


        file.set_len(0)?;
        let mut output_writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        let read_buffer = &mut [0u8; (CHUNK_AVG_SIZE * 4) as usize]; // 4 倍平均块大小的缓冲区

        let mut file_cache = HashMap::new();

        struct LastRead {
            pack_id: String,
            read_offset: (u64, u64), // 读取偏移和长度
        }

        let mut last_read = LastRead {
            pack_id: String::new(),
            read_offset: (0, 0),
        };

        // 遍历 Hash 列表，读取并拼接
        let mut file_size = 0;
        let pack_dir = self.pack_dir.as_ref().unwrap();
        for hash in chunks {
            let table_bytes = match chunk_table.get(&hash)? {
                Some(bytes) => bytes,
                None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("缺少块数据，Hash: {}", hash)))),
            };

            let location: ChunkLocation = bitcode::decode(table_bytes.value())?;
            file_cache.entry(location.pack_id.to_string()).or_insert_with(|| {
                let path = pack_dir.join(&location.pack_id);
                File::open(path).unwrap()
            });
            
            file_size += location.length;

            // 定位数据块，检查能否和上次读取的块合并
            if last_read.pack_id == location.pack_id && last_read.read_offset.0 + last_read.read_offset.1 == location.offset {
                // 可以合并读取
                last_read.read_offset.1 += location.length as u64;
                continue;
            } 
            else {
                // 不能合并读取，先读取上次的块，然后更新为当前块
                if let Some(file) = file_cache.get_mut(&last_read.pack_id) {
                    // 读取上次的块
                    let length = last_read.read_offset.1 as usize;
                    write_file(file, &mut output_writer, last_read.read_offset.0, length, read_buffer)?;
                }

                // 更新为当前块
                last_read.pack_id = location.pack_id.to_string();
                last_read.read_offset = (location.offset, location.length as u64);
            }
        }

        // 读取并写入最后一个块
        if let Some(file) = file_cache.get_mut(&last_read.pack_id) {
            let length = last_read.read_offset.1 as usize;
            write_file(file, &mut output_writer, last_read.read_offset.0, length, read_buffer)?;
        }

        Ok(file_size)
    }

}


/// 计算数据的 SHA256 Hash
pub fn calculate_hash(data: &[u8]) -> String {
    let hash = blake2::Blake2b::<U32>::digest(data);
    hex_util::encode_hex(&hash)
}

/// 写入文件
pub fn write_file( origin_file: &mut File, output_file: &mut BufWriter<&File>, start_offset: u64, length: usize, read_buffer: &mut [u8]) -> Result<(), Box<dyn std::error::Error>> {
    origin_file.seek(SeekFrom::Start(start_offset))?;
    let mut total_read = 0usize;
    while total_read < length {
        let read_size = std::cmp::min(read_buffer.len(), length - total_read);
        let n = origin_file.read(&mut read_buffer[..read_size])?;
        if n == 0 {
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "读取文件时遇到意外的 EOF")));
        }
        output_file.write_all(&read_buffer[..n])?;
        total_read += n;
    }
    Ok(())
}
