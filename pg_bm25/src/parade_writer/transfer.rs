use interprocess::os::unix::fifo_file;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, StreamDeserializer};
use std::fs::File;
use std::io::{BufReader, Write};
use std::marker::PhantomData;
use std::os::unix::prelude::PermissionsExt;
use std::path::{Path, PathBuf};

#[derive(Deserialize, Serialize)]
pub enum WriterTransferMessage<T> {
    Data(T),
    Done,
}

pub struct WriterTransferMessageIterator<'a, T> {
    stream:
        StreamDeserializer<'a, serde_json::de::IoRead<BufReader<File>>, WriterTransferMessage<T>>,
}

impl<'a, T> Iterator for WriterTransferMessageIterator<'a, T>
where
    T: DeserializeOwned + 'a,
{
    type Item = serde_json::Result<WriterTransferMessage<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stream.next() {
            Some(Ok(WriterTransferMessage::Data(builder))) => {
                pgrx::log!("GOT MESSAGE");
                Some(Ok(WriterTransferMessage::Data(builder)))
            }
            Some(Ok(WriterTransferMessage::Done)) => {
                pgrx::log!("GOT DONE MESSAGE");
                None // End interator
            }
            Some(Err(e)) => {
                pgrx::log!("Error parsing JSON in writer transfer consumer message: {e:?}",);
                None // End iterator on error
            }
            None => None, // No more items
        }
    }
}

pub struct WriterTransferProducer<T: Serialize> {
    pipe: File,
    pipe_path: PathBuf,
    marker: PhantomData<T>,
}

impl<T: Serialize> WriterTransferProducer<T> {
    pub fn new() -> std::io::Result<Self> {
        let pipe_path = crate::env::paradedb_transfer_pipe_path();
        let pipe = Self::create_named_pipe_file(&pipe_path)?;
        Ok(Self {
            pipe,
            pipe_path,
            marker: PhantomData,
        })
    }

    pub fn write_message(&mut self, data: &T) -> std::io::Result<()> {
        pgrx::log!("WRITING MESSAGE!");
        let message = WriterTransferMessage::Data(data);
        let serialized = serde_json::to_vec(&message)?;
        self.write_all(&serialized)?;
        self.flush()
    }

    pub fn write_done_message(&mut self) -> std::io::Result<()> {
        pgrx::log!("WRITING DONE MESSAGE!");
        let message: WriterTransferMessage<T> = WriterTransferMessage::Done;
        let serialized = serde_json::to_vec(&message).unwrap();
        self.write_all(&serialized)?;
        self.flush()
    }

    fn create_named_pipe_file(pipe_path: &Path) -> std::io::Result<File> {
        if pipe_path.exists() {
            std::fs::remove_file(&pipe_path)?;
        }

        fifo_file::create_fifo(&pipe_path, 0o600)?;

        let permissions = std::fs::Permissions::from_mode(0o666);
        std::fs::set_permissions(&pipe_path, permissions)?;

        File::create(&pipe_path)
    }
}

impl<T: Serialize> Write for WriterTransferProducer<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.pipe.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.pipe.flush()
    }
}

impl<T: Serialize> Drop for WriterTransferProducer<T> {
    fn drop(&mut self) {
        let pipe_path = self.pipe_path.clone();
        if let Err(err) = self.write_done_message() {
            pgrx::log!("error sending writer transfer done message: {err:?}")
        };
        if let Err(err) = std::fs::remove_file(&pipe_path) {
            pgrx::log!("error removing named pipe path {pipe_path:?}: {err:?}");
        }
    }
}

pub struct WriterTransferConsumer {
    pipe_path: PathBuf,
}

impl WriterTransferConsumer {
    pub fn new() -> std::io::Result<Self> {
        let pipe_path = crate::env::paradedb_transfer_pipe_path();
        // We'll remove the existing pipe_path, because we want to allow
        // the producer to create the file.
        if pipe_path.exists() {
            std::fs::remove_file(&pipe_path).unwrap_or_else(|err| {
                pgrx::log!(
                    "writer consumer could not remove the pipe path file {pipe_path:?}: {err:?}"
                )
            });
        }
        Ok(Self { pipe_path })
    }

    pub fn read_stream<'a, T>(&'a mut self) -> WriterTransferMessageIterator<'a, T>
    where
        T: DeserializeOwned + 'a,
    {
        // Wait for the client to create the pipe.
        while !self.pipe_path.exists() {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let pipe_file = std::fs::OpenOptions::new()
            .read(true)
            // .custom_flags(libc::O_NONBLOCK) // Set the O_NONBLOCK flag
            .open(&self.pipe_path)
            .unwrap_or_else(|err| {
                let pipe_path = self.pipe_path.display().to_string();
                panic!("could not open pipe file at {pipe_path}: {err:?}");
            });

        let reader = BufReader::new(pipe_file);
        let stream = Deserializer::from_reader(reader).into_iter::<WriterTransferMessage<T>>();
        WriterTransferMessageIterator { stream }
    }
}
