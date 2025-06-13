use std::path::Path;
use std::path::StripPrefixError;

use path_slash::PathExt;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum PathConversionsError {
    #[error("strip prefix error")]
    StripPrefixError(#[from] StripPrefixError),
}

/// Create cloud path from given local
pub fn make_s3_key(
    source_root: &Path,
    full_path: &Path,
    s3_prefix: &str,
) -> Result<String, PathConversionsError> {
    let rel_path = full_path.strip_prefix(source_root)?;
    let unix_path = rel_path.to_slash_lossy();
    Ok(format!("{}/{}", s3_prefix.trim_end_matches('/'), unix_path))
}

/// Normalize the log output so it always uses `/`, even on Windows
pub fn normalize_path<P: AsRef<Path>>(path: P) -> String {
    path.as_ref().to_slash_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rstest::rstest;

    use super::*;

    // On non-Windows, convert backslashes to slashes before creating PathBuf
    fn normalize_path_helper(path: &str) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(path)
        } else {
            PathBuf::from(path.replace('\\', "/"))
        }
    }

    #[rstest]
    #[case(&Path::new("path/"), &Path::new("path/to/data/"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    #[case(&Path::new("path"), &Path::new("path/to/data/"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    #[case(&Path::new("/path/"), &Path::new("/path/to/data/"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    #[case(&Path::new("/path"), &Path::new("/path/to/data/"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    #[case(&Path::new("//path/"), &Path::new("/path/to/data/"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    #[case(&Path::new("//path"), &Path::new("/path/to/data/"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    #[case(&Path::new(""), &Path::new("path/to/data/"), "cloud/path/to/data/", Ok("cloud/path/to/data/path/to/data".to_string()))]
    #[case(&normalize_path_helper(r"path\"), &normalize_path_helper(r"path\to\data\"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    #[case(&normalize_path_helper(r"\\path\"), &normalize_path_helper(r"\\path\to\data\"), "cloud/path/to/data/", Ok("cloud/path/to/data/to/data".to_string()))]
    fn make_s3_key_test(
        #[case] source_root: &Path,
        #[case] full_path: &Path,
        #[case] s3_prefix: &str,
        #[case] expected: Result<String, PathConversionsError>,
    ) {
        assert_eq!(expected, make_s3_key(source_root, full_path, s3_prefix));
    }

    #[rstest]
    #[case(&Path::new("foo"), &Path::new("/bar/to/data/"), "cloud/path/to/data/", true)]
    #[case(&Path::new("path"), &Path::new("/path/to/data/"), "cloud/path/to/data/", true)]
    #[case(&Path::new("/pathpath"), &Path::new("/path/to/data/"), "cloud/path/to/data/", true)]
    fn make_s3_key_test_err(
        #[case] source_root: &Path,
        #[case] full_path: &Path,
        #[case] s3_prefix: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(
            expected,
            make_s3_key(source_root, full_path, s3_prefix).is_err()
        );
    }

    #[rstest]
    #[case(&Path::new("path/to/data/"), "path/to/data/".to_string())]
    #[case(&Path::new("/path/to/data/"), "/path/to/data/".to_string())]
    #[case(&Path::new("//path/to/data/"), "//path/to/data/".to_string())]
    #[case(&Path::new("//path/to/data/"), "//path/to/data/".to_string())]
    #[case(&normalize_path_helper(r"\\path\to\data\"), "//path/to/data/".to_string())]
    #[case(&normalize_path_helper(r"\path\to\data\"), "/path/to/data/".to_string())]
    fn normalize_path_test(#[case] path: &Path, #[case] expected: String) {
        assert_eq!(expected, normalize_path(path));
    }
}
