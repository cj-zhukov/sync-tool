pub fn sanitize_file_path(path: &str) -> String {
    path
        .replace('\\', "/")
}