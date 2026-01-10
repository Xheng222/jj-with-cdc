use std::{collections::HashMap, fs::File, io::{BufWriter, Read, Seek, SeekFrom, Write}, path::PathBuf};

use memmap2::Mmap;
use redb::{Database, ReadOnlyTable, ReadTransaction, ReadableDatabase};

use crate::cdc::cdc_config::{BUFFER_SIZE, CHUNK_DB, GLOBAL_LOCK, HASH_LENGTH, MAX_PACK_SIZE, PACKS_DIR, TABLE_CHUNKS};



#[derive(Debug, Clone)]
pub struct ChunkLocation {
    pub pack_id: u32, // 属于哪个 pack 文件
    pub offset: u32,     // 偏移量
    pub len_idx: u16,     // 长度
    pub reference_count: u16, // 引用计数
}

impl ChunkLocation {
    fn to_bytes(self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..4].copy_from_slice(&self.pack_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.offset.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.len_idx.to_be_bytes());
        bytes[10..12].copy_from_slice(&self.reference_count.to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; 12]) -> Self {
        Self {
            pack_id: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            offset: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            len_idx: u16::from_be_bytes(bytes[8..10].try_into().unwrap()),
            reference_count: u16::from_be_bytes(bytes[10..12].try_into().unwrap()),
        }
    }
}

pub trait ChunkIndexBackend {
    /// 获取 chunk 的位置
    fn get(&mut self, hash: &[u8; HASH_LENGTH]) -> Result<Option<ChunkLocation>, Box<dyn std::error::Error>>;

    /// 移除 chunk 的位置
    fn _remove(&mut self, hash: &[u8; HASH_LENGTH]) -> Result<(), Box<dyn std::error::Error>>;

    /// 更新 chunk 的位置
    /// - 如果 chunk 已经存在，则更新其位置
    /// - 如果 chunk 不存在，则添加
    fn update(&mut self, hash: &[u8; HASH_LENGTH], location: ChunkLocation) -> Result<(), Box<dyn std::error::Error>>;
}

pub trait ChunkWriterBackend {
    /// 写入 chunk 数据
    fn write_chunk(&mut self, data: &[u8]) -> Result<ChunkLocation, Box<dyn std::error::Error>>;

    /// 读取 chunk 文件的 mmap
    fn read_chunk_file_mmap(&mut self, pack_id: u32) -> Result<Mmap, Box<dyn std::error::Error>>;    
}

pub trait ChunkBackend: ChunkIndexBackend + ChunkWriterBackend + Send {}

struct ChunkWriter {
    pack_id: u32,
    current_size: u32,
    file_writer: Option<BufWriter<File>>,
    lock_file: File,
    store_path: PathBuf,
}

impl ChunkWriter {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.pack_id.to_be_bytes());
        bytes.extend_from_slice(&self.current_size.to_be_bytes());
        bytes
    }

    fn new(store_path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(store_path.join(GLOBAL_LOCK))?;

        let mut bytes = Vec::new();
        file.lock()?;
        file.read_to_end(&mut bytes)?;

        let pack_id = u32::from_be_bytes(bytes.get(0..4).unwrap_or(&[0; 4]).try_into().unwrap());
        let current_size = u32::from_be_bytes(bytes.get(4..8).unwrap_or(&[0; 4]).try_into().unwrap());

        Ok(Self {
            pack_id,
            current_size,
            file_writer: None,
            lock_file: file,
            store_path: store_path.join(PACKS_DIR),
        })
    }

    fn init_writer(&mut self) -> Result<(), std::io::Error>{
        if self.file_writer.is_some() {
            return Ok(());
        }
        let pack_path = self.store_path.join(&self.pack_id.to_string());
        let file_writer = BufWriter::with_capacity(
            BUFFER_SIZE,
            std::fs::OpenOptions::new().create(true).write(true).append(true).open(&pack_path)?
        );
        self.file_writer = Some(file_writer);
        Ok(())
    }

    fn switch_new_pack(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.pack_id += 1;
        self.current_size = 0;
        self.file_writer.take().unwrap().flush()?;
        Ok(())
    }
}

impl Drop for ChunkWriter {
    fn drop(&mut self) {
        if let Some(mut file_writer) = self.file_writer.take() {
            file_writer.flush().ok();
        }
        let writer_bytes = self.to_bytes();
        self.lock_file.seek(SeekFrom::Start(0)).ok();
        self.lock_file.write_all(&writer_bytes).ok();
        self.lock_file.flush().ok();
        self.lock_file.unlock().ok();
    }
}

impl ChunkWriterBackend for ChunkWriter {
    fn write_chunk(&mut self, data: &[u8]) -> Result<ChunkLocation, Box<dyn std::error::Error>> {
        self.init_writer()?;
        self.file_writer.as_mut().unwrap().write_all(data)?;

        let location = ChunkLocation {
            pack_id: self.pack_id,
            offset: self.current_size,
            len_idx: (data.len() - 1) as u16,
            reference_count: 1,
        };

        self.current_size += data.len() as u32;

        if self.current_size >= MAX_PACK_SIZE {
            self.switch_new_pack()?;
        }

        Ok(location)
    }
    
    #[allow(unsafe_code)]
    fn read_chunk_file_mmap(&mut self, pack_id: u32) -> Result<Mmap, Box<dyn std::error::Error>> {
        let pack_path = self.store_path.join(pack_id.to_string());
        let file = File::open(pack_path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(mmap)
    }
}


pub struct RedbChunkBackend {
    chunk_db: Database,
    chunk_table: ReadOnlyTable<[u8; HASH_LENGTH], [u8; 12]>,
    mem_cache_table: HashMap<[u8; HASH_LENGTH], ChunkLocation>,
    chunk_writer: ChunkWriter,
}

impl ChunkBackend for RedbChunkBackend {}

impl RedbChunkBackend {
    pub fn new(store_path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_writer = ChunkWriter::new(store_path)?;
        let chunk_db = match Database::open(store_path.join(CHUNK_DB)) {
            Ok(db) => db,
            Err(_) => {
                let db = Database::create(store_path.join(CHUNK_DB))?;
                let write_txn = db.begin_write()?;
                {
                    write_txn.open_table(TABLE_CHUNKS)?;
                }
                write_txn.commit()?;
                db
            }
        };

        let table_rx: ReadTransaction = chunk_db.begin_read()?;
        let chunk_table = table_rx.open_table(TABLE_CHUNKS)?;

        Ok(Self {
            chunk_db: chunk_db,
            chunk_table: chunk_table,
            mem_cache_table: HashMap::new(),
            chunk_writer: chunk_writer,
        })
    }

    pub fn flush_mem_cache(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let chunk_wtx = self.chunk_db.begin_write()?;
        {
            let mut chunk_table = chunk_wtx.open_table(TABLE_CHUNKS)?;
            for (hash, location) in self.mem_cache_table.drain() {
                chunk_table.insert(hash, location.to_bytes())?;
            }
        }

        chunk_wtx.commit()?;
        self.chunk_table = self.chunk_db.begin_read()?.open_table(TABLE_CHUNKS)?;
        Ok(())
    }
}

impl ChunkWriterBackend for RedbChunkBackend {
    fn write_chunk(&mut self, data: &[u8]) -> Result<ChunkLocation, Box<dyn std::error::Error>> {
        self.chunk_writer.write_chunk(data)
    }
    
    fn read_chunk_file_mmap(&mut self, pack_id: u32) -> Result<Mmap, Box<dyn std::error::Error>> {
        self.chunk_writer.read_chunk_file_mmap(pack_id)
    }
}


impl ChunkIndexBackend for RedbChunkBackend {
    fn get(&mut self, hash: &[u8; HASH_LENGTH]) -> Result<Option<ChunkLocation>, Box<dyn std::error::Error>> {
        // 先查 mem_cache_table，再查 chunk_table
        if let Some(location) = self.mem_cache_table.get(hash) {
            return Ok(Some(location.clone()));
        }

        let location_bytes = match self.chunk_table.get(hash) {
            Ok(guard) => {
                match guard {
                    Some(guard) => guard.value(),
                    None => return Ok(None),
                }
            },
            Err(e) => return Err(e.into()),
        };
        Ok(Some(ChunkLocation::from_bytes(location_bytes)))
    }

    fn update(&mut self, hash: &[u8; HASH_LENGTH], location: ChunkLocation) -> Result<(), Box<dyn std::error::Error>> {
        self.mem_cache_table.insert(*hash, location);
        if self.mem_cache_table.len() >= 65535 {
            self.flush_mem_cache()?;
        }
        Ok(())
    }

    fn _remove(&mut self, hash: &[u8; HASH_LENGTH]) -> Result<(), Box<dyn std::error::Error>> {
        self.mem_cache_table.remove(hash);
        Ok(())
    }
}

impl Drop for RedbChunkBackend {
    fn drop(&mut self) {
        self.flush_mem_cache().ok();
    }
}
