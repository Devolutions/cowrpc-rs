use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let base_path: PathBuf = if cfg!(target_os = "windows") {
        let path = Path::new("C:\\wayk\\sdk\\Windows");
        if cfg!(target_pointer_width = "32") {
            path.join("x86")
        } else {
            path.join("x64")
        }
    } else if cfg!(target_os = "linux") {
        let path = Path::new("/opt/wayk/sdk/Linux");
        if cfg!(target_pointer_width = "32") {
            path.join("i386")
        } else {
            path.join("amd64")
        }
    } else {
        Path::new("/opt/wayk/sdk/macOS").to_path_buf()
    };

    let open_ssl_dir = base_path.join("openssl");

    let open_ssl_lib_dir = open_ssl_dir.join("lib");

    let open_ssl_include_dir = open_ssl_dir.join("include");

    env::set_var("OPENSSL_DIR", open_ssl_dir.as_os_str());
    env::set_var("OPENSSL_LIB_DIR", open_ssl_lib_dir.as_os_str());
    env::set_var("OPENSSL_INCLUDE_DIR", open_ssl_include_dir.as_os_str());
}
