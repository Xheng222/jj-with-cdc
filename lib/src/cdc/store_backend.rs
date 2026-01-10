use std::{collections::HashMap, fs::File, io::{BufWriter, Write}, path::PathBuf, sync::mpsc, thread};

use fastcdc::v2020::{Chunk, FastCDC};
use itertools::Itertools;
use memmap2::Mmap;
use rayon::{iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelBridge, ParallelDrainRange, ParallelIterator}};
use crate::cdc::{cdc_config::{BUFFER_SIZE, CHUNK_AVG_SIZE, HASH_LENGTH, PACKS_DIR}, chunk_backend::{ChunkBackend, RedbChunkBackend}, manifest_backend::{CdcManifest, GitManifestBackend, ManifestBackend}, pointer::{CdcPointer, CdcPointerBytes}, utils::calculate_hash};


pub trait StoreBackend: Send {
    fn write_file(&mut self, file: &File) ->Result<CdcPointerBytes, Box<dyn std::error::Error>>;
    fn load_file(&mut self, pointer: &CdcPointer, file: &File) -> Result<usize, Box<dyn std::error::Error>>;
}

struct PendingCdcChunk<'a> {
    hash: [u8; HASH_LENGTH],
    data: &'a [u8],
}

pub struct ChunkStoreBackend {
    chunk_backend: Box<dyn ChunkBackend>,
    manifest_backend: Box<dyn ManifestBackend>,
}


impl ChunkStoreBackend {
    pub fn new(store_path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let pack_dir = store_path.join(PACKS_DIR);
        if !pack_dir.exists() {
            std::fs::create_dir_all(&pack_dir)?;
        }
        let chunk_backend = RedbChunkBackend::new(store_path)?;
        let manifest_backend = GitManifestBackend::new(store_path)?;


        Ok(Self {
            chunk_backend: Box::new(chunk_backend),
            manifest_backend: Box::new(manifest_backend),
        })
    }

    #[inline]
    fn flush(&mut self, mmap: &[u8], chunks: &mut Vec<Chunk>, manifest: &mut CdcManifest) -> Result<(), Box<dyn std::error::Error>> {
        let pending_chunks: Vec<PendingCdcChunk> = chunks.par_drain(..).map(|chunk| {
            let data = &mmap[chunk.offset .. chunk.offset + chunk.length];
            let hash = calculate_hash(data);
            PendingCdcChunk {
                hash: hash[0..HASH_LENGTH].try_into().unwrap(),
                data: data,
            }
        }).collect();
        
        for pending_chunk in pending_chunks {
            let location = 
                if let Some(mut location) = self.chunk_backend.get(&pending_chunk.hash)? {
                    location.reference_count += 1;
                    location
                }
                else {
                    self.chunk_backend.write_chunk(&pending_chunk.data)?
                };
            self.chunk_backend.update(&pending_chunk.hash, location)?;
            manifest.push(pending_chunk.hash);
        }
        Ok(())
    }
}

impl StoreBackend for ChunkStoreBackend {
    #[allow(unsafe_code)]
    fn write_file(&mut self, file: &File) -> Result<CdcPointerBytes, Box<dyn std::error::Error>> {
        let mmap = unsafe {
            memmap2::MmapOptions::new().map(file)?
        };



        // 遍历切片
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut manifest: CdcManifest = Vec::new();
        let mut chunks_length = 0u32;

        let (tx, rx) = mpsc::sync_channel(1024);

        thread::spawn(move || {
            let all_chunks = FastCDC::new(
                &mmap,
                CHUNK_AVG_SIZE / 4,
                CHUNK_AVG_SIZE,
                CHUNK_AVG_SIZE * 4
            );            
            for chunk in &all_chunks.chunks(1024) {
                tx.send(chunk).unwrap();
            }
        });

        for chunk in &all_chunks.chunks(1024) {
            chunks.push(chunk);
            chunks_length += chunk.length as u32;

            if chunks_length >= MAX_PACK_SIZE {
                chunks_length = 0;
                self.flush(&mmap, &mut chunks, &mut manifest)?;
            }

            chunk.collect::<Vec<Chunk>>().par_drain(..).map(|chunk| {
                let data = &mmap[chunk.offset .. chunk.offset + chunk.length];
                let hash = calculate_hash(data);
                PendingCdcChunk {
                    hash: hash[0..HASH_LENGTH].try_into().unwrap(),
                    data: data,
                }
            }).collect::<Vec<PendingCdcChunk>>();

        }

        self.flush(&mmap, &mut chunks, &mut manifest)?;

        let pointer = self.manifest_backend.write_manifest(manifest)?;

        Ok(pointer)
    }

    fn load_file(&mut self, pointer: &CdcPointer, file: &File) -> Result<usize, Box<dyn std::error::Error>> {
        let manifest = self.manifest_backend.read_manifest(pointer)?;

        file.set_len(0)?;
        let mut output_writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        let mut file_cache = HashMap::new();

        struct LastRead {
            pack_id: u32,
            read_offset: u32,
            read_length: u32,
        }

        let mut last_read = LastRead {
            pack_id: 0,
            read_offset: 0,
            read_length: 0
        };

        let mut file_size = 0usize;
        for hash in manifest {
            let location = match self.chunk_backend.get(&hash)? {
                Some(location) => location,
                None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("缺少块数据，Hash: {:?}", hash)))),
            };

            if !file_cache.contains_key(&location.pack_id) {
                file_cache.insert(location.pack_id, self.chunk_backend.read_chunk_file_mmap(location.pack_id)?);
            }
            
            file_size += location.len_idx as usize + 1;

            // 定位数据块，检查能否和上次读取的块合并
            if last_read.pack_id == location.pack_id && last_read.read_offset + last_read.read_length == location.offset {
                // 可以合并读取
                last_read.read_length += location.len_idx as u32 + 1;
                continue;
            } 
            else {
                // 不能合并读取，先读取上次的块，然后更新为当前块
                if let Some(mmap) = file_cache.get(&last_read.pack_id) && last_read.read_length > 0 {
                    // 读取上次的块
                    let length = last_read.read_length as usize;
                    write_file(mmap, &mut output_writer, last_read.read_offset as usize, length)?;
                }

                // 更新为当前块
                last_read.pack_id = location.pack_id;
                last_read.read_offset = location.offset;
                last_read.read_length = location.len_idx as u32 + 1;
            }
        }

        // 读取并写入最后一个块
        if let Some(mmap) = file_cache.get(&last_read.pack_id) && last_read.read_length > 0 {
            // 读取上次的块
            let length = last_read.read_length as usize;
            write_file(mmap, &mut output_writer, last_read.read_offset as usize, length)?;
        }

        output_writer.flush()?;

        Ok(file_size)
    }
}

#[inline]
fn write_file(mmap: &Mmap, output_writer: &mut BufWriter<&File>, start_offset: usize, length: usize) -> Result<(), Box<dyn std::error::Error>> {
    output_writer.write_all(&mmap[start_offset..start_offset + length])?;
    Ok(())
}



