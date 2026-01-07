#![expect(missing_docs)]

use std::path::PathBuf;

pub trait ChunkIndexHashBackend {}

pub struct IndexManager {
    base_path: PathBuf,
}
