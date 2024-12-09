use crate::shared::PlatformError;
use anyhow::{anyhow, Context};
use std::fs::DirBuilder;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

pub fn private_directory(root: &Path) -> Result<(), PlatformError> {
    let result = DirBuilder::new().mode(0o700).create(root);
    match result {
        Ok(_) => {}
        Err(err) => {
            if err.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(PlatformError::DirectoryCreationError(
                    root.to_path_buf(),
                    anyhow!("DirBuilder failed with {}", err),
                ));
            }
            if !root.is_dir() {
                return Err(PlatformError::DirectoryCreationError(
                    root.to_path_buf(),
                    anyhow!("target exists but is not a directory"),
                ));
            }
            let permissions = root
                .metadata()
                .context("failed to get metadata")
                .map_err(|err| PlatformError::DirectoryPermissionError(root.to_path_buf(), err))?
                .permissions();
            let mode = permissions.mode() & 0o777; // Only check the permissions, not the type.
            if mode != 0o700 {
                return Err(PlatformError::DirectoryPermissionError(
                    root.to_path_buf(),
                    anyhow!("unexpected mode {:o}", mode),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_config_dir_with_permissions() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let parent = tempfile::TempDir::new()?;
        let path = parent.path().join("config");
        private_directory(&path)?;

        // Directory permission is expected to be rwx------ aka 700.
        let metadata = path.metadata()?;
        let permissions = metadata.permissions();

        // mode is the full permission, which includes bits for the inode
        // type, etc.
        assert_eq!(permissions.mode() & 0o777, 0o700);

        // We can then call the function again just fine.
        private_directory(&path)?;

        Ok(())
    }

    #[test]
    fn reject_invalid_permissions() -> anyhow::Result<()> {
        use std::fs::DirBuilder;
        use std::os::unix::fs::DirBuilderExt;

        let parent = tempfile::TempDir::new()?;
        let path = parent.path().join("config");
        DirBuilder::new().mode(0o770).create(&path)?;

        assert!(matches!(
            private_directory(&path),
            Err(PlatformError::DirectoryPermissionError(_, _))
        ));
        Ok(())
    }
}
