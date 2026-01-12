
use std::path::PathBuf;

use gix::{Id, ObjectId, Repository};

use crate::cdc::{
        cdc_config::{HASH_LENGTH, MANIFEST_ANCHOR_REF, MANIFEST_GIT_DIR}, cdc_error::{CdcError, CdcResult}, pointer::{CdcPointer, CdcPointerBytes} 
    };

pub type CdcManifest = Vec<[u8; HASH_LENGTH]>;

pub trait ManifestBackend: Send {
    /// 写入 manifest，返回 manifest 的 hash
    fn write_manifest(&self, manifest: CdcManifest) -> CdcResult<CdcPointerBytes>;
    /// 读取 manifest
    fn read_manifest(&self, pointer: &CdcPointer) -> CdcResult<CdcManifest>;
    /// 移除 manifest
    fn _remove_manifest(&self, pointer: &CdcPointer) -> CdcResult<()>;
    /// GC
    fn gc(&self, keep_manifests: &[CdcPointer]) -> CdcResult<()>;

}

pub struct GitManifestBackend {
    inner: Repository,

}

impl GitManifestBackend {
    pub fn new(store_path: &PathBuf) -> CdcResult<Self> {
        let manifest_git_path = store_path.join(MANIFEST_GIT_DIR);
        let manifest_git = match gix::open(&manifest_git_path) {
            Ok(git) => git,
            Err(_) => {
                let git = gix::init_bare(&manifest_git_path).map_err(CdcError::from_git)?;
                {
                    let empty_tree = git.empty_tree().id; // 空树 id
                    let mut editor = git.edit_tree(empty_tree).map_err(CdcError::from_git)?;
                    let oid = editor.write().map_err(CdcError::from_git)?;
                    git.reference(
                        MANIFEST_ANCHOR_REF,
                        oid,
                        gix::refs::transaction::PreviousValue::MustNotExist,
                        "init gixkv anchor",
                    ).map_err(CdcError::from_git)?;
                }
                git
            },
        };

        Ok(Self {
            inner: manifest_git,
        })
    }

    fn get_manifest_tree_oid<'a>(&'a self) -> CdcResult<Id<'a>> {
        let mut r = self.inner.find_reference(MANIFEST_ANCHOR_REF).map_err(CdcError::from_git)?;
        let id = r.peel_to_id().map_err(CdcError::from_git)?;
        Ok(id)
    }

    fn set_manifest_tree_oid<'a>(&'a self, new_tree: Id<'a>) -> CdcResult<()> {
        let mut r = self.inner.find_reference(MANIFEST_ANCHOR_REF).map_err(CdcError::from_git)?;
        r.set_target_id(
            new_tree,
            "set manifest tree oid"
        ).map_err(CdcError::from_git)?;
        Ok(())
    }
}

impl ManifestBackend for GitManifestBackend {
    fn write_manifest(&self, manifest: CdcManifest) -> CdcResult<CdcPointerBytes> {
        let manifest_bytes = encode_manifest_raw(manifest);
        let blob_id = self.inner.write_blob(manifest_bytes).map_err(CdcError::from_git)?;

        let manifest_hash = blob_id.to_string();
        let (prefix, suffix) = split_manifest_hash(&manifest_hash);

        let manifest_tree_oid = self.get_manifest_tree_oid()?;
        let mut editor = self.inner.edit_tree(manifest_tree_oid).map_err(CdcError::from_git)?;
        editor.upsert(format!("{prefix}/{suffix}"), gix::object::tree::EntryKind::Blob, blob_id)
            .map_err(CdcError::from_git)?;
        let new_tree_oid = editor.write().map_err(CdcError::from_git)?;
        self.set_manifest_tree_oid(new_tree_oid)?;

        tracing::debug!("CDC Manifest Backend: Written manifest {:?}", manifest_hash);
        tracing::debug!("CDC Manifest Backend: Written manifest len {:?}", manifest_hash.as_bytes().len());

        Ok(CdcPointer::new(manifest_hash).serialize())
    }

    fn read_manifest(&self, pointer: &CdcPointer) -> CdcResult<CdcManifest> {
        let oid = oid_from_hex(pointer.hash())?;
        let blob = self.inner.find_blob(oid).map_err(CdcError::from_git)?;
        let manifest = decode_manifest_raw(&blob.data);
        Ok(manifest)
    }

    fn _remove_manifest(&self, pointer: &CdcPointer) -> CdcResult<()> {
        let (prefix, suffix) = split_manifest_hash(pointer.hash());
        let manifest_tree_oid = self.get_manifest_tree_oid()?;
        let mut editor = self.inner.edit_tree(manifest_tree_oid).map_err(CdcError::from_git)?;
        editor.remove(format!("{prefix}/{suffix}")).map_err(CdcError::from_git)?;
        let new_tree_oid = editor.write().map_err(CdcError::from_git)?;
        self.set_manifest_tree_oid(new_tree_oid)?;
        Ok(())
    }

    fn gc(&self, keep_manifests: &[CdcPointer]) -> CdcResult<()> {

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

fn oid_from_hex(hex: &str) -> CdcResult<ObjectId> {
    let oid = ObjectId::from_hex(hex.as_bytes()).map_err(CdcError::from_git)?;
    Ok(oid)
}
