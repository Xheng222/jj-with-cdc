#![expect(missing_docs)]

use std::{fs::File, path::Path, pin::Pin, time::SystemTime};

use futures::stream::BoxStream;
use tokio::{io::AsyncRead, sync::Mutex};
use tracing::debug;

use crate::{
    backend::{Backend, BackendResult, ChangeId, Commit, CommitId, CopyHistory, CopyId, CopyRecord, FileId, SigningFn, SymlinkId, Tree, TreeId}, 
    cdc::{cdc_magager::CdcMagager, pointer::CdcPointer}, git_backend::{GitBackend, GitBackendLoadError}, 
    index::Index, 
    repo_path::{RepoPath, RepoPathBuf}, 
    settings::UserSettings, working_copy::CheckoutError
};


/// CDC backend wrapper
#[derive(Debug)]
pub struct CdcBackendWrapper {
    /// The underlying backend 
    inner: GitBackend,
    /// The CDC manager
    cdc_manager: Mutex<CdcMagager>,
}

impl CdcBackendWrapper {
    pub fn name() -> &'static str {
        "git"
    }

    pub fn load(
        settings: &UserSettings,
        store_path: &Path,
    ) -> Result<Self, Box<GitBackendLoadError>> {
        let inner = GitBackend::load(settings, store_path)?;
        debug!("store_path: {}", store_path.display());
        Ok(Self { inner, cdc_manager: Mutex::new(CdcMagager::new(store_path.to_path_buf().join("cdc"))) })
    }

    pub fn inner(&self) -> &GitBackend {
        &self.inner
    }

    pub async fn write_file_to_cdc(&self, file: &mut File) -> Vec<u8> {
        let mut cdc_manager = self.cdc_manager.lock().await;
        match cdc_manager.write_file_to_cdc(file) {
            Ok(pointer_content) => pointer_content,
            Err(e) => {
                debug!("Failed to write file to CDC: {:?}", e);
                return Vec::new();
            }
        }
    }

    pub async fn read_file_from_cdc(&self, pointer_content: &CdcPointer, file: &mut File) -> Result<usize, CheckoutError> {
        let mut cdc_manager = self.cdc_manager.lock().await;
        match cdc_manager.read_file_from_cdc(pointer_content, file) {
            Ok(size) => Ok(size),
            Err(e) => {
                debug!("Failed to read file from CDC: {:?}", e);
                return Err(CheckoutError::Other {
                    message: format!("Failed to read file from CDC: {:?}", e),
                    err: Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())),
                });
            }
        }
    }
}

impl Backend for CdcBackendWrapper {
    fn read_file<'life0,'life1,'life2,'async_trait>(&'life0 self,path: &'life1 RepoPath,id: &'life2 FileId,) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<Pin<Box<dyn AsyncRead+Send> > > > + ::core::marker::Send+'async_trait> >
        where 'life0:'async_trait,'life1:'async_trait,'life2:'async_trait,Self:'async_trait 
    {
        debug!("CDC Backend Wrapper: Reading file {}", path.as_internal_file_string());
        self.inner.read_file(path, id)
        // TODO: Implement CDC read file
    }

    fn write_file<'life0,'life1,'life2,'async_trait>(&'life0 self,path: &'life1 RepoPath,contents: &'life2 mut (dyn AsyncRead+Send+Unpin),) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<FileId> > + ::core::marker::Send+'async_trait> >
        where 'life0:'async_trait,'life1:'async_trait,'life2:'async_trait,Self:'async_trait 
    {
        debug!("CDC Backend Wrapper: Writing file {}", path.as_internal_file_string());
        self.inner.write_file(path, contents)
    }

    fn name(&self) ->  &str {
        self.inner.name()
    }

    fn commit_id_length(&self) -> usize {
        self.inner.commit_id_length()
    }

    fn change_id_length(&self) -> usize {
        self.inner.change_id_length()
    }

    fn root_commit_id(&self) ->  &CommitId {
        self.inner.root_commit_id()
    }

    fn root_change_id(&self) ->  &ChangeId {
        self.inner.root_change_id()
    }

    fn empty_tree_id(&self) ->  &TreeId {
        self.inner.empty_tree_id()
    }

    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }

    fn read_symlink<'life0,'life1,'life2,'async_trait>(&'life0 self,path: &'life1 RepoPath,id: &'life2 SymlinkId) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<String> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,'life2:'async_trait,Self:'async_trait {
        self.inner.read_symlink(path, id)
    }

    fn write_symlink<'life0,'life1,'life2,'async_trait>(&'life0 self,path: &'life1 RepoPath,target: &'life2 str) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<SymlinkId> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,'life2:'async_trait,Self:'async_trait {
        self.inner.write_symlink(path, target)
    }

    fn read_copy<'life0,'life1,'async_trait>(&'life0 self,id: &'life1 CopyId) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<CopyHistory> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,Self:'async_trait {
        debug!("CDC Backend Wrapper: Reading copy {:?}", id);
        self.inner.read_copy(id)
    }

    fn write_copy<'life0,'life1,'async_trait>(&'life0 self,copy: &'life1 CopyHistory) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<CopyId> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,Self:'async_trait {
        debug!("CDC Backend Wrapper: Writing copy {:?}", copy);
        self.inner.write_copy(copy)
    }

    fn get_related_copies<'life0,'life1,'async_trait>(&'life0 self,copy_id: &'life1 CopyId) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<Vec<CopyHistory> > > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,Self:'async_trait {
        self.inner.get_related_copies(copy_id)
    }

    fn read_tree<'life0,'life1,'life2,'async_trait>(&'life0 self,path: &'life1 RepoPath,id: &'life2 TreeId) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<Tree> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,'life2:'async_trait,Self:'async_trait {
        self.inner.read_tree(path, id)
    }

    fn write_tree<'life0,'life1,'life2,'async_trait>(&'life0 self,path: &'life1 RepoPath,contents: &'life2 Tree) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<TreeId> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,'life2:'async_trait,Self:'async_trait {
        self.inner.write_tree(path, contents)
    }

    fn read_commit<'life0,'life1,'async_trait>(&'life0 self,id: &'life1 CommitId) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<Commit> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,Self:'async_trait {
        self.inner.read_commit(id)
    }

    fn write_commit<'life0,'life1,'async_trait>(&'life0 self, contents: Commit, sign_with: Option< &'life1 mut SigningFn> ) ->  ::core::pin::Pin<Box<dyn ::core::future::Future<Output = BackendResult<(CommitId,Commit)> > + ::core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,Self:'async_trait {
        self.inner.write_commit(contents, sign_with)
    }

    fn get_copy_records(&self,paths:Option< &[RepoPathBuf]> ,root: &CommitId,head: &CommitId) -> BackendResult<BoxStream<'_,BackendResult<CopyRecord> > >  {
        self.inner.get_copy_records(paths, root, head)
    }

    fn gc(&self,index: &dyn Index,keep_newer:SystemTime) -> BackendResult<()>  {
        self.inner.gc(index, keep_newer)
    }
}

