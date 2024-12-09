mod shared;

// Import the platform-specific module. They are all expected to expose
// the same public APIs. This avoids the need to pepper the code with
// #[cfg(windows)] everywhere.

#[cfg(windows)]
mod win;
#[cfg(windows)]
use win as platform;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as platform;

// Expose the public API at the top-level.
pub use platform::private_directory;
pub use shared::PlatformError;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn create_directory() -> anyhow::Result<()> {
        let td = TempDir::new()?;
        let target = td.path().join("target");

        private_directory(&target)?;
        assert!(target.is_dir());
        // The existing directory is a valid target.
        private_directory(&target)?;
        Ok(())
    }

    #[test]
    fn detect_creation_errors() -> anyhow::Result<()> {
        let td = TempDir::new()?;
        let target = td.path().join("target");
        let fd = std::fs::File::create(&target)?;
        drop(fd);

        let err = private_directory(&target);
        match err {
            Ok(_) => {
                panic!("This was meant to fail")
            }
            Err(PlatformError::DirectoryPermissionError(_, _)) => {
                panic!("Should be a creation error, got {:?}", err)
            }
            Err(PlatformError::DirectoryCreationError(path, _)) => {
                assert_eq!(path, target);
            }
            Err(PlatformError::UnexpectedError(_)) => {
                panic!("Should be a creation error, got {:?}", err)
            }
        }
        Ok(())
    }

    #[test]
    fn directory_must_be_secure() -> anyhow::Result<()> {
        let td = TempDir::new()?;

        let err = private_directory(td.path());
        match err {
            Ok(_) => {
                panic!("This was meant to fail")
            }
            Err(PlatformError::DirectoryPermissionError(path, _)) => {
                assert_eq!(path, td.path());
            }
            Err(PlatformError::DirectoryCreationError(_, _)) => {
                panic!("Should be a permission error, got {:?}", err)
            }
            Err(PlatformError::UnexpectedError(_)) => {
                panic!("Should be a permission error, got {:?}", err)
            }
        }
        Ok(())
    }
}
