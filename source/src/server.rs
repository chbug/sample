use self::{builder::Builder, peer::Peer};
use crate::state::{Change, Store};
use anyhow::Result;
use crypto::{self, model};
use std::path::PathBuf;
use storage::filesystem::{AsyncFileOps, ShallowInfo, WalkEvent};
use storage::fingerprint::Fingerprinter;
use tokio::sync::mpsc;

mod builder;
mod peer;

/// A Source server, which watches the filesystem and backs data up to a Sink.
pub struct Server<P: Peer> {
    roots: Vec<PathBuf>,
    peer: P,
    fops: AsyncFileOps,
    fp: Fingerprinter,
    store: Store,
    source_key: crypto::Keys,
}

const CHUNK_SIZE: usize = 2usize.pow(22); // 23 breaks the current gRPC limit.

pub fn builder() -> Builder {
    Builder::default()
}

impl<P: Peer> Server<P> {
    /// Run the server. For now, the server shuts down after scanning the filesystem once.
    pub async fn serve(self) -> Result<()> {
        self.single_pass().await?;

        self.store.shutdown().await?;
        Ok(())
    }

    /// Checks the filesystem once, and sends any changed files to the Sink.
    async fn single_pass(&self) -> Result<()> {
        tracing::info!("starting full check on {:?}", &self.roots);

        // Synchronous single pass: read all the files, chunk them.
        let (tx, mut rx) = mpsc::channel(1);
        let walk_op = self.fops.walk(self.roots.clone(), tx);
        tokio::pin!(walk_op);

        let mut done = false;
        while !done {
            tokio::select! {
                Some(update) = rx.recv() => {
                    match update {
                        WalkEvent::File(info) => {
                            // For now, abort at the first failure, to be able to detect.
                            // TODO: distinguish permanent errors (db error for instance) and
                            // transient issues.
                            self.single_file(&info).await?;
                        },
                        WalkEvent::Error(path, err) => {
                            tracing::info!("error accessing {:?}: {:?}", path, err);
                        },
                    }
                },
                walk_done = &mut walk_op, if !done => {
                    done = true;
                    if let Err(err) = walk_done {
                        tracing::error!("fs walk failure: {:?}", err);
                    }
                },
            };
        }
        Ok(())
    }

    /// Checks a single file, and sends it to the Sink if it has changed.
    #[tracing::instrument(skip(self))]
    async fn single_file(&self, info: &ShallowInfo) -> Result<()> {
        match self.store.check_shallow_change(info).await? {
            Change::Changed(_) => {
                tracing::info!("sending changed file");
            }
            Change::Unchanged(_) => {
                tracing::info!("skipping unchanged file");
                return Ok(());
            }
        }
        let version = self.store.insert(info).await?;
        let _descriptor = self.source_key.encrypt_descriptor(
            model::VerifiedDescriptor {
                file_id: version.file_id,
                version: version.version,
                index: 0,
                total: 1,
                chunks: vec![],
            },
            model::ProtectedDescriptor {
                filename: info.file().to_string_lossy().to_string(),
                size: info.len(),
            },
        )?;

        let (chunk_in, mut chunk_out) = mpsc::channel(1);

        let reader = self.fops.read_chunks(info.file(), CHUNK_SIZE, chunk_in);
        tokio::pin!(reader);

        loop {
            tokio::select! {
                Some(data) = chunk_out.recv() => {
                    let chunk = self.fp.hash(data).await;
                    tracing::info!("hashed to {:?}", chunk.digest());
                    if let Err(err) = self.peer.send(&chunk).await {
                        tracing::error!("failed to send chunk: {:?}", err);
                    }
                }

                done = &mut reader => {
                    match done {
                        Ok(size) => {
                            tracing::info!("hashed and inserted file of size {}", size);
                            self.store.insert(info).await?;
                            return Ok(());
                        },
                        Err(err) => {
                            tracing::error!("failed to read file: {:?}", err);
                            return Err(err);
                        }
                    }
                }
            }
        }
    }
}
