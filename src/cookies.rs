use anyhow::{Context, Result};
use std::{
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

pub fn import_cookie(source: &Path, dest: &Path) -> Result<()> {
    let parent = dest.parent().context("cookies 目标路径没有父目录")?;
    let name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cookies");
    let temporary = parent.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut input = std::fs::File::open(source)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        output.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        std::io::copy(&mut input, &mut output)?;
        output.sync_all()?;
        drop(output);
        std::fs::rename(&temporary, dest)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cookie_import_atomically_replaces_with_private_file() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.txt");
        let dest = directory.path().join("cookies.txt");
        std::fs::write(&source, "new-cookie").unwrap();
        std::fs::write(&dest, "old-cookie").unwrap();
        assert!(import_cookie(&directory.path().join("missing"), &dest).is_err());
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "old-cookie");
        import_cookie(&source, &dest).unwrap();

        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "new-cookie");
        assert_eq!(
            std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(
            std::fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
        );
    }
}
