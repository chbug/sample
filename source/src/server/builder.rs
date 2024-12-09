use super::{peer, Server};
use crate::state::Store;
use settings::connection;
use source_settings::Settings;
use std::path::{Path, PathBuf};
use storage::filesystem::AsyncFileOps;
use storage::fingerprint::{self, Fingerprinter};

#[derive(Default)]
pub struct Builder {
    roots: Vec<PathBuf>,
    connection: Option<connection::Info>,
    broker: Option<broker_client::Settings>,
    db: PathBuf,
    rnd: Option<crypto::SharedRandom>,
    source_key: Option<crypto::Keys>,
}

#[derive(thiserror::Error, Debug)]
pub enum BuilderError {
    #[error("Missing connection settings")]
    MissingConnection,
    #[error("Missing Broker info")]
    MissingBrokerInfo,
    #[error("Missing Crypto")]
    MissingCrypto,
    #[error("Invalid fingerprinter")]
    FingerprinterError(#[from] fingerprint::FingerprinterError),
    #[error("Unknown error")]
    UnknownError(#[from] anyhow::Error),
}

impl Builder {
    pub fn settings(self, settings: &Settings) -> Builder {
        self.roots(settings.backup().roots().clone())
            .connection(settings.connection())
            .broker(settings.broker())
            .db(settings.backup().db())
    }

    pub fn roots(mut self, roots: Vec<PathBuf>) -> Builder {
        self.roots = roots;
        self
    }

    pub fn connection(mut self, connection: &connection::Settings) -> Builder {
        self.connection = Some(connection.info().clone());
        self
    }

    pub fn broker(mut self, broker: &broker_client::Settings) -> Builder {
        self.broker = Some((*broker).clone());
        self
    }

    pub fn db(mut self, path: &Path) -> Builder {
        self.db = path.to_path_buf();
        self
    }

    pub fn crypto(mut self, rnd: crypto::SharedRandom, source_key: crypto::Keys) -> Builder {
        self.rnd = Some(rnd);
        self.source_key = Some(source_key);
        self
    }

    pub async fn build(self) -> Result<Server<peer::PeerImpl>, BuilderError> {
        let connection = self.connection.ok_or(BuilderError::MissingConnection)?;
        let broker_info = self.broker.ok_or(BuilderError::MissingBrokerInfo)?;
        let broker = broker_client::new(&connection, &broker_info).await?;
        let peer = peer::new(broker, connection);
        let rnd = self.rnd.ok_or(BuilderError::MissingCrypto)?;
        let source_key = self.source_key.ok_or(BuilderError::MissingCrypto)?;

        Ok(Server {
            roots: self.roots,
            peer,
            fops: AsyncFileOps::new().await,
            fp: Fingerprinter::new(1)?,
            store: Store::new(&self.db, rnd).await?,
            source_key,
        })
    }
}
