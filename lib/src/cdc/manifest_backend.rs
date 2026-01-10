
use std::path::PathBuf;

use gix::{Id, ObjectId, Repository};

use crate::{
    cdc::{
        cdc_config::{HASH_LENGTH, MANIFEST_ANCHOR_REF, MANIFEST_GIT_DIR}, 
        pointer::{CdcPointer, CdcPointerBytes}, 
    },
};

pub type CdcManifest = Vec<[u8; HASH_LENGTH]>;

pub trait ManifestBackend: Send {
    /// 写入 manifest，返回 manifest 的 hash
    fn write_manifest(&self, manifest: CdcManifest) -> Result<CdcPointerBytes, Box<dyn std::error::Error>>;
    /// 读取 manifest
    fn read_manifest(&self, pointer: &CdcPointer) -> Result<CdcManifest, Box<dyn std::error::Error>>;
    /// 移除 manifest
    fn _remove_manifest(&self, pointer: &CdcPointer) -> Result<(), Box<dyn std::error::Error>>;
    /// GC
    fn _gc(&self) -> Result<(), Box<dyn std::error::Error>>;

}

pub struct GitManifestBackend {
    inner: Repository,

}

impl GitManifestBackend {
    pub fn new(store_path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let manifest_git_path = store_path.join(MANIFEST_GIT_DIR);
        let manifest_git = match gix::open(&manifest_git_path) {
            Ok(git) => git,
            Err(_) => {
                let git = gix::init_bare(&manifest_git_path)?;
                {
                    let empty_tree = git.empty_tree().id; // 空树 id
                    let mut editor = git.edit_tree(empty_tree)?;
                    let oid = editor.write()?;
                    git.reference(
                        MANIFEST_ANCHOR_REF,
                        oid,
                        gix::refs::transaction::PreviousValue::MustNotExist,
                        "init gixkv anchor",
                    )?;
                }
                git
            },
        };

        Ok(Self {
            inner: manifest_git,
        })
    }

    fn get_manifest_tree_oid<'a>(&'a self) -> Result<Id<'a>, Box<dyn std::error::Error>> {
        let mut r = self.inner.find_reference(MANIFEST_ANCHOR_REF)?;
        let id = r.peel_to_id()?;
        Ok(id)
    }

    fn set_manifest_tree_oid<'a>(&'a self, new_tree: Id<'a>) -> Result<(), Box<dyn std::error::Error>> {
        let mut r = self.inner.find_reference(MANIFEST_ANCHOR_REF)?;
        r.set_target_id(
            new_tree, 
            "set manifest tree oid"
        )?;
        Ok(())
    }
}

impl ManifestBackend for GitManifestBackend {
    fn write_manifest(&self, manifest: CdcManifest) -> Result<CdcPointerBytes, Box<dyn std::error::Error>> {
        let manifest_bytes = encode_manifest_raw(manifest);
        let blob_id = self.inner.write_blob(manifest_bytes)?;

        let manifest_hash = blob_id.to_string();
        let (prefix, suffix) = split_manifest_hash(&manifest_hash);
    
        let manifest_tree_oid = self.get_manifest_tree_oid()?;
        let mut editor = self.inner.edit_tree(manifest_tree_oid)?;
        editor.upsert(
            format!("{prefix}/{suffix}"), 
            gix::object::tree::EntryKind::Blob, 
            blob_id
        )?;
        let new_tree_oid = editor.write()?;
        self.set_manifest_tree_oid(new_tree_oid)?;

        Ok(CdcPointer::new(manifest_hash).serialize())
    }

    fn read_manifest(&self, pointer: &CdcPointer) -> Result<CdcManifest, Box<dyn std::error::Error>> {
        let oid = oid_from_hex(pointer.hash())?;
        let blob = self.inner.find_blob(oid)?;
        let manifest = decode_manifest_raw(&blob.data);
        Ok(manifest)
    }

    fn _remove_manifest(&self, pointer: &CdcPointer) -> Result<(), Box<dyn std::error::Error>> {
        let (prefix, suffix) = split_manifest_hash(pointer.hash());
        let manifest_tree_oid = self.get_manifest_tree_oid()?;
        let mut editor = self.inner.edit_tree(manifest_tree_oid)?;
        editor.remove(format!("{prefix}/{suffix}"))?;
        let new_tree_oid = editor.write()?;
        self.set_manifest_tree_oid(new_tree_oid)?;
        Ok(())
    }

    fn _gc(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

fn encode_manifest_raw(chunk_hashes: CdcManifest) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunk_hashes.len() * HASH_LENGTH);
    for h in chunk_hashes {
        out.extend(h);
    }
    out
}

fn decode_manifest_raw(bytes: &[u8]) -> CdcManifest {
    let mut out = Vec::with_capacity(bytes.len() / HASH_LENGTH);
    for chunk in bytes.chunks_exact(HASH_LENGTH) {
        let mut h = [0u8; HASH_LENGTH];
        h.copy_from_slice(chunk);
        out.push(h);
    }
    out
}

fn split_manifest_hash(key: &str) -> (&str, &str) {
    let (prefix, suffix) = key.split_at(2);
    (prefix, suffix)
}

fn oid_from_hex(hex: &str) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let oid = ObjectId::from_hex(hex.as_bytes())?;
    Ok(oid)
}
