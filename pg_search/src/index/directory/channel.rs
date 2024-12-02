use anyhow::Result;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use parking_lot::Mutex;
use pgrx::pg_sys;
use rustc_hash::FxHashMap;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{io, io::Write, ops::Range, result};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, TerminatingWrite, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::index::SegmentMetaInventory;
use tantivy::{Directory, IndexMeta};

use crate::index::directory::blocking::BlockingDirectory;
use crate::index::reader::channel::ChannelReader;
use crate::index::reader::segment_component::SegmentComponentReader;
use crate::index::writer::channel::ChannelWriter;
use crate::index::writer::segment_component::SegmentComponentWriter;
use crate::postgres::storage::block::{bm25_max_free_space, DirectoryEntry};

pub enum ChannelRequest {
    ListManagedFiles(oneshot::Sender<HashSet<PathBuf>>),
    RegisterFilesAsManaged(Vec<PathBuf>, bool),
    SegmentRead(Range<usize>, DirectoryEntry, oneshot::Sender<Vec<u8>>),
    SegmentWrite(PathBuf, Vec<u8>),
    SegmentWriteTerminate(PathBuf),
    GetSegmentComponent(PathBuf, oneshot::Sender<DirectoryEntry>),
    SaveMetas(IndexMeta),
    LoadMetas(SegmentMetaInventory, oneshot::Sender<IndexMeta>),
    Terminate,
}

#[derive(Clone, Debug)]
pub struct ChannelDirectory {
    sender: Sender<ChannelRequest>,
}

// A directory that actually forwards all read/write requests to a channel
// This channel is used to communicate with the actual storage implementation
impl ChannelDirectory {
    pub fn new(sender: Sender<ChannelRequest>) -> Self {
        Self { sender }
    }
}

impl Directory for ChannelDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(unsafe {
            ChannelReader::new(path, self.sender.clone()).map_err(|e| {
                OpenReadError::wrap_io_error(
                    io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
                    path.to_path_buf(),
                )
            })?
        }))
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        Ok(io::BufWriter::with_capacity(
            unsafe { bm25_max_free_space() },
            Box::new(unsafe { ChannelWriter::new(path, self.sender.clone()) }),
        ))
    }

    /// atomic_write is used by Tantivy to write to managed.json, meta.json, and create .lock files
    /// This function should never be called by our Tantivy fork because we write to managed.json and meta.json ourselves
    fn atomic_write(&self, path: &Path, _data: &[u8]) -> io::Result<()> {
        unimplemented!("atomic_write should not be called for {:?}", path);
    }

    /// atomic_read is used by Tantivy to read from managed.json and meta.json
    /// This function should never be called by our Tantivy fork because we read from them ourselves
    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        unimplemented!("atomic_read should not be called for {:?}", path);
    }

    // This is called by Tantivy's garbage collect process, which we do not want to implement
    // because we use Postgres MVCC rules for our own garbage collection in amvacuumcleanup
    fn delete(&self, _path: &Path) -> result::Result<(), DeleteError> {
        Ok(())
    }

    // Internally, Tantivy only uses this for meta.json, which should always exist
    fn exists(&self, _path: &Path) -> Result<bool, OpenReadError> {
        Ok(true)
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(Lock {
            filepath: lock.filepath.clone(),
            is_blocking: true,
        })))
    }

    // Internally, tantivy only uses this API to detect new commits to implement the
    // `OnCommitWithDelay` `ReloadPolicy`. Not implementing watch in a `Directory` only prevents
    // the `OnCommitWithDelay` `ReloadPolicy` to work properly.
    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        unimplemented!("OnCommitWithDelay ReloadPolicy not supported");
    }

    // Block storage handles disk writes for us, we don't need to fsync
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn list_managed_files(&self) -> tantivy::Result<HashSet<PathBuf>> {
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        self.sender
            .send(ChannelRequest::ListManagedFiles(oneshot_sender))
            .unwrap();

        Ok(oneshot_receiver.recv().unwrap())
    }

    fn register_files_as_managed(
        &self,
        files: Vec<PathBuf>,
        overwrite: bool,
    ) -> tantivy::Result<()> {
        self.sender
            .send(ChannelRequest::RegisterFilesAsManaged(files, overwrite))
            .unwrap();

        Ok(())
    }

    fn save_metas(&self, meta: &IndexMeta) -> tantivy::Result<()> {
        self.sender
            .send(ChannelRequest::SaveMetas(meta.clone()))
            .unwrap();

        Ok(())
    }

    fn load_metas(&self, inventory: &SegmentMetaInventory) -> tantivy::Result<IndexMeta> {
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        self.sender
            .send(ChannelRequest::LoadMetas(inventory.clone(), oneshot_sender))
            .unwrap();

        Ok(oneshot_receiver.recv().unwrap())
    }
}

type Action = Box<dyn FnOnce() -> Reply + Send + Sync>;
type Reply = Box<dyn Any + Send + Sync>;
pub struct ChannelRequestHandler {
    directory: BlockingDirectory,
    relation_oid: pg_sys::Oid,
    receiver: Receiver<ChannelRequest>,
    writers: Mutex<FxHashMap<PathBuf, SegmentComponentWriter>>,
    readers: Mutex<FxHashMap<PathBuf, SegmentComponentReader>>,

    action: (Sender<Action>, Receiver<Action>),
    reply: (Sender<Reply>, Receiver<Reply>),
    _worker: JoinHandle<()>,
}

pub type ShouldTerminate = bool;

impl ChannelRequestHandler {
    pub fn open(
        directory: BlockingDirectory,
        relation_oid: pg_sys::Oid,
        receiver: Receiver<ChannelRequest>,
    ) -> Self {
        let (action_sender, action_receiver) = crossbeam::channel::bounded(1);
        let (reply_sender, reply_receiver) = crossbeam::channel::bounded(1);
        Self {
            directory,
            relation_oid,
            receiver,
            writers: Default::default(),
            readers: Default::default(),
            action: (action_sender, action_receiver.clone()),
            reply: (reply_sender.clone(), reply_receiver),
            _worker: std::thread::spawn(move || {
                for message in action_receiver {
                    if reply_sender.send(message()).is_err() {
                        // channel was dropped and that's okay
                        break;
                    }
                }
            }),
        }
    }

    pub fn wait_for<T: Send + Sync + 'static, F: FnOnce() -> T + Send + Sync + 'static>(
        &self,
        func: F,
    ) -> Result<T> {
        let func: Action = Box::new(move || Box::new(func()));
        self.action.0.send(func)?;
        loop {
            match self.reply.1.try_recv() {
                // `func` has finished and we have its reply
                Ok(reply) => {
                    return match reply.downcast::<T>() {
                        // the reply is exactly what we hoped for
                        Ok(reply) => Ok(*reply),

                        // it's something else, so transform into a generic error
                        Err(e) => Err(anyhow::anyhow!("unexpected reply {:?}", e)),
                    };
                }

                // we have no reply yet, so process any messages it may have generated
                Err(TryRecvError::Empty) => {
                    for message in self.receiver.try_iter() {
                        match self.process_message(message) {
                            Ok(should_terminate) if should_terminate => break,
                            Ok(_) => continue,
                            Err(e) => return Err(e),
                        }
                    }
                }

                // the reply channel was closed, so lets just return that as the error
                Err(TryRecvError::Disconnected) => {
                    return Err(anyhow::anyhow!("reply channel disconnected"));
                }
            }
        }
    }

    pub fn receive_blocking(self) -> Result<()> {
        let receiver = self.receiver.clone();
        for message in receiver {
            self.process_message(message)?;
        }

        Ok(())
    }

    fn process_message(&self, message: ChannelRequest) -> Result<ShouldTerminate> {
        match message {
            ChannelRequest::ListManagedFiles(sender) => {
                let managed_files = self.directory.list_managed_files()?;
                sender.send(managed_files)?;
            }
            ChannelRequest::RegisterFilesAsManaged(files, overwrite) => {
                self.directory.register_files_as_managed(files, overwrite)?;
            }
            ChannelRequest::GetSegmentComponent(path, sender) => {
                let (opaque, _, _) = unsafe { self.directory.directory_lookup(&path)? };
                sender.send(opaque)?;
            }
            ChannelRequest::SegmentRead(range, handle, sender) => {
                let mut mutex = self.readers.lock();
                let reader = mutex.entry(handle.path.clone()).or_insert_with(|| unsafe {
                    SegmentComponentReader::new(self.relation_oid, handle)
                });
                let data = reader.read_bytes(range)?;
                drop(mutex);
                sender.send(data.as_slice().to_owned())?;
            }
            ChannelRequest::SegmentWrite(path, data) => {
                let mut mutex = self.writers.lock();
                let writer = mutex.entry(path.clone()).or_insert_with(|| unsafe {
                    SegmentComponentWriter::new(self.relation_oid, &path)
                });
                writer.write_all(&data)?;
            }
            ChannelRequest::SegmentWriteTerminate(path) => {
                let mut mutex = self.writers.lock();
                let writer = mutex.remove(&path).expect("writer should exist");
                writer.terminate()?;
            }
            ChannelRequest::SaveMetas(metas) => {
                self.directory.save_metas(&metas)?;
            }
            ChannelRequest::LoadMetas(inventory, sender) => {
                let metas = self.directory.load_metas(&inventory)?;
                sender.send(metas)?;
            }
            ChannelRequest::Terminate => {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
