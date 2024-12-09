//! Exceptions and other common elements.

#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("The directory {0} could not be created: {1}")]
    DirectoryCreationError(std::path::PathBuf, anyhow::Error),
    #[error("The directory {0} has unexpected permission: {1}")]
    DirectoryPermissionError(std::path::PathBuf, anyhow::Error),
    #[error("Unexpected: {0:?}")]
    UnexpectedError(anyhow::Error),
}

impl From<anyhow::Error> for PlatformError {
    fn from(err: anyhow::Error) -> PlatformError {
        PlatformError::UnexpectedError(err)
    }
}
