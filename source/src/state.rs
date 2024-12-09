use anyhow::{Context, Result};
use crypto::model::{self, FileId};
use crypto::{self, RandomApi};
use rusqlite::{params, Connection};
use std::{path::Path, sync::Arc};
use storage::filesystem::ShallowInfo;
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

#[derive(Debug)]
pub struct Store {
    handle: JoinHandle<Result<(), anyhow::Error>>,
    tx: Sender<StateOp>,
}

#[derive(Debug, PartialEq)]
pub struct Partial {
    pub file_id: model::FileId,
    pub version: model::Version,
    pub len: u64,
}

/// Whether a change took place in the file or not.  
#[derive(Debug, PartialEq)]
pub enum Change {
    /// This file has not changed.
    Unchanged(Partial),
    /// This file has changed. Partial is None if the file was
    /// previously unknown.
    Changed(Option<Partial>),
}

impl Store {
    /// Open or create a permanent Store.
    pub async fn new(db_path: &'_ Path, rnd: crypto::SharedRandom) -> anyhow::Result<Self> {
        let db = Connection::open(db_path)?;
        Store::initialize(db, rnd)
            .await
            .context("failed to initialize")
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.tx); // Make the runner stop.
        self.handle.await?
    }

    /// Perform a shallow change check, based on the file's metadata.
    pub async fn check_shallow_change(&self, info: &ShallowInfo) -> anyhow::Result<Change> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StateOp::ShallowCheck(info.clone(), tx))
            .await
            .context("failed to send command")?;
        rx.await.context("failed to get result")?
    }

    pub async fn insert(&self, info: &ShallowInfo) -> anyhow::Result<Partial> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StateOp::Insert(info.clone(), tx))
            .await
            .context("failed to send command")?;
        rx.await.context("failed to get result")?
    }

    /// Creates a new in-memory Store.
    #[cfg(test)]
    pub async fn new_for_test(rnd: Arc<dyn RandomApi + Send + Sync>) -> anyhow::Result<Self> {
        let db = Connection::open_in_memory()?;
        Store::initialize(db, rnd).await
    }

    async fn initialize(
        db: Connection,
        rnd: Arc<dyn RandomApi + Send + Sync>,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel::<StateOp>(1);
        let mut runner = StoreRunner { db, rnd };
        let handle = tokio::task::spawn_blocking(move || {
            let result = runner.run(rx);
            if let Err(err) = result {
                tracing::error!("StoreRunner failed: {:?}", err);
            }
            Ok(())
        });
        Ok(Store { handle, tx })
    }
}

struct StoreRunner {
    db: Connection,
    rnd: Arc<dyn RandomApi + Send + Sync>,
}

impl StoreRunner {
    fn run(&mut self, mut rx: Receiver<StateOp>) -> anyhow::Result<()> {
        self.db.execute(
            "
        CREATE TABLE IF NOT EXISTS FileId (
            id BLOB PRIMARY KEY,
            -- Path is not necessarily a valid UTF-8 string, so we store it as a BLOB.
            path BLOB
        ) STRICT, WITHOUT ROWID;",
            (),
        )?;
        self.db.execute(
            "
        CREATE UNIQUE INDEX IF NOT EXISTS idx_file_path ON FileId(path);",
            (),
        )?;

        self.db.execute(
            "
        CREATE TABLE IF NOT EXISTS File (
            id      BLOB NOT NULL,
            version INTEGER NOT NULL,
            len     INTEGER NOT NULL,
            PRIMARY KEY (id, version)
        ) STRICT, WITHOUT ROWID;",
            (),
        )?;

        loop {
            match rx.blocking_recv() {
                None => break,

                Some(StateOp::ShallowCheck(info, tx)) => {
                    tx.send(self.shallow_check(&info)).unwrap();
                }

                Some(StateOp::Insert(info, tx)) => {
                    tx.send(self.insert(&info)).unwrap();
                }
            }
        }
        Ok(())
    }

    fn shallow_check(&mut self, info: &ShallowInfo) -> anyhow::Result<Change> {
        let tx = self.db.transaction()?;
        let file_id = get_file_id(self.rnd.as_ref(), info.file(), &tx)?;
        let partial = lookup_file(&file_id, &tx)?;
        tx.commit()?;

        match partial {
            None => Ok(Change::Changed(None)),
            Some(partial) => {
                if partial.len != info.len() {
                    Ok(Change::Changed(Some(partial)))
                } else {
                    Ok(Change::Unchanged(partial))
                }
            }
        }
    }

    fn insert(&mut self, info: &ShallowInfo) -> anyhow::Result<Partial> {
        let tx = self.db.transaction()?;
        let file_id = get_file_id(self.rnd.as_ref(), info.file(), &tx)?;

        let version = match lookup_file(&file_id, &tx)? {
            None => 0,
            Some(partial) => partial.version + 1,
        };
        tx.execute(
            "
        INSERT INTO File(id, version, len) VALUES(?1, ?2, ?3) 
        ",
            (file_id.as_bytes(), version, info.len()),
        )?;
        tx.commit()?;
        Ok(Partial {
            file_id,
            version,
            len: info.len(),
        })
    }
}

fn get_file_id(
    rnd: &dyn crypto::RandomApi,
    path: &Path,
    tx: &rusqlite::Transaction,
) -> anyhow::Result<FileId> {
    let canonical = canonical_path_repr(path);
    let mut stmt = tx.prepare("SELECT id FROM FileId WHERE path = ?1")?;
    let mut ids = stmt.query_map((&canonical,), |row| row.get::<usize, Vec<u8>>(0))?;

    if let Some(id) = ids.next() {
        return FileId::try_from(id?.as_slice());
    }

    // If the file is not found, insert a new random value for it.
    let mut stmt = tx.prepare("INSERT INTO FileId(id, path) VALUES(?1, ?2)")?;
    let mut id = rnd.generate_file_id()?;
    let mut success = false;
    // Ensure there is no collision in the random id for the new file.
    while !success {
        match stmt.execute(params![id.as_bytes(), &canonical]) {
            Ok(_) => {
                success = true;
            }
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                id = rnd.generate_file_id()?;
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(id)
}

fn lookup_file(
    file_id: &model::FileId,
    tx: &rusqlite::Transaction,
) -> anyhow::Result<Option<Partial>> {
    let mut stmt =
        tx.prepare("SELECT version, len FROM File WHERE id = ?1 ORDER BY version DESC LIMIT 1")?;
    let mut rows = stmt.query_map((file_id.as_bytes(),), |row| {
        Ok(Partial {
            file_id: file_id.clone(),
            version: row.get(0)?,
            len: row.get(1)?,
        })
    })?;

    if let Some(row) = rows.next() {
        Ok(Some(row?))
    } else {
        Ok(None)
    }
}

#[derive(Debug)]
enum StateOp {
    Insert(ShallowInfo, oneshot::Sender<anyhow::Result<Partial>>),
    ShallowCheck(ShallowInfo, oneshot::Sender<anyhow::Result<Change>>),
}

#[cfg(windows)]
fn canonical_path_repr(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    let wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    let mut canonical: Vec<u8> = Vec::with_capacity(wide.len() * 2);
    // The information is specific to the platform it originated from, so there is no need to handle endianness or
    // map between Unix & Windows, etc.
    for pair in wide {
        canonical.push((pair & 0xFF) as u8);
        canonical.push(((pair >> 8) & 0xFF) as u8);
    }
    canonical
}

#[cfg(unix)]
fn canonical_path_repr(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    let mut canonical: Vec<u8> = Vec::new();
    canonical.extend_from_slice(path.as_os_str().as_bytes());
    canonical
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug, Default)]
    struct TestRandom {
        value: Arc<Mutex<Vec<u8>>>,
    }
    impl RandomApi for TestRandom {
        fn generate_file_id(&self) -> Result<FileId> {
            let mut value = self.value.lock().unwrap();
            if value.is_empty() {
                Err(anyhow::anyhow!("Ran out of random values"))
            } else {
                let next = value.pop().unwrap();
                Ok(model::FileId::try_from([next; 6].as_slice())?)
            }
        }

        fn generate_block_id(&self) -> anyhow::Result<model::BlockId> {
            todo!()
        }
    }
    impl TestRandom {
        fn new<T: AsRef<[u8]>>(values: T) -> Self {
            TestRandom {
                // Revert the order to pop from the front later on while still
                // keeping the original order.
                value: Arc::new(Mutex::new(values.as_ref().iter().copied().rev().collect())),
            }
        }
    }

    #[tokio::test]
    async fn smoke_test() -> anyhow::Result<()> {
        let rnd = Arc::new(TestRandom::new(vec![0]));
        let db = Store::new_for_test(rnd).await.expect("creation failed");

        let file = ShallowInfo::new(Path::new("a/b").to_owned(), 100);

        let change = db.check_shallow_change(&file).await?;
        assert_eq!(change, Change::Changed(None));

        let expected = Partial {
            file_id: FileId::default(),
            version: 0,
            len: 100,
        };
        assert_eq!(db.insert(&file).await?, expected);
        assert_eq!(
            db.check_shallow_change(&file).await?,
            Change::Unchanged(expected)
        );

        db.shutdown().await
    }

    #[tokio::test]
    async fn insert_new_version() -> anyhow::Result<()> {
        let rnd = Arc::new(TestRandom::new(vec![0]));
        let db = Store::new_for_test(rnd).await.expect("creation failed");

        db.insert(&ShallowInfo::new(Path::new("a/b").to_owned(), 100))
            .await?;

        let file = ShallowInfo::new(Path::new("a/b").to_owned(), 200);
        db.insert(&file).await?;

        let change = db.check_shallow_change(&file).await?;
        assert_eq!(
            change,
            Change::Unchanged(Partial {
                file_id: FileId::default(),
                version: 1,
                len: 200
            })
        );

        db.shutdown().await
    }

    #[tokio::test]
    async fn file_id_collision() -> anyhow::Result<()> {
        // Pop the same value twice, then a different one. This should cause one retry for the
        // second file, but ultimately succeed.
        let rnd = Arc::new(TestRandom::new(vec![1, 1, 2]));
        let db = Store::new_for_test(rnd).await.expect("creation failed");

        let file_b = ShallowInfo::new(Path::new("a/b").to_owned(), 100);
        db.insert(&file_b).await?;

        let file_c = ShallowInfo::new(Path::new("a/c").to_owned(), 100);
        db.insert(&file_c).await?;

        assert_eq!(
            db.check_shallow_change(&file_b).await?,
            Change::Unchanged(Partial {
                file_id: FileId::try_from(vec![1; 6].as_slice())?,
                version: 0,
                len: 100
            })
        );
        assert_eq!(
            db.check_shallow_change(&file_c).await?,
            Change::Unchanged(Partial {
                file_id: FileId::try_from(vec![2; 6].as_slice())?,
                version: 0,
                len: 100
            })
        );
        db.shutdown().await
    }
}
