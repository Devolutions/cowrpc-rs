use std::io::BufReader;
use std::{fs, io};
use tokio_rustls::rustls;

pub fn load_certs(certificate_file_path: &str) -> io::Result<Vec<rustls::Certificate>> {
    let cert_file = fs::File::open(certificate_file_path)?;
    let mut reader = BufReader::new(cert_file);

    rustls::internal::pemfile::certs(&mut reader)
        .map_err(|()| io::Error::new(io::ErrorKind::InvalidData, "Failed to parse certificate"))
}

pub fn load_private_key(private_key_file_path: &str) -> io::Result<rustls::PrivateKey> {
    let mut pkcs8_keys = load_pkcs8_private_key(private_key_file_path)?;

    // prefer to load pkcs8 keys
    if !pkcs8_keys.is_empty() {
        Ok(pkcs8_keys.remove(0))
    } else {
        let mut rsa_keys = load_rsa_private_key(private_key_file_path)?;

        assert!(!rsa_keys.is_empty());
        Ok(rsa_keys.remove(0))
    }
}

fn load_rsa_private_key(private_key_file_path: &str) -> io::Result<Vec<rustls::PrivateKey>> {
    let keyfile = fs::File::open(private_key_file_path)?;
    rustls::internal::pemfile::rsa_private_keys(&mut BufReader::new(keyfile))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "File contains invalid rsa private key"))
}

fn load_pkcs8_private_key(private_key_file_path: &str) -> io::Result<Vec<rustls::PrivateKey>> {
    let keyfile = fs::File::open(private_key_file_path)?;
    rustls::internal::pemfile::pkcs8_private_keys(&mut BufReader::new(keyfile)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "File contains invalid pkcs8 private key (encrypted keys not supported)",
        )
    })
}
