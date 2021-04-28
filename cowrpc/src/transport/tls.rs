use tls_api::Certificate;

use crate::error::{CowRpcError, Result};

/// A version the TLS protocol.
pub enum TlsVersion {
    V12,
}

/// A certificate and associated password in the PKCS#12 format.
pub struct Pkcs12 {
    pub der: Vec<u8>,
    pub password: String,
}

/// An identification method (certificate).
pub enum Identity {
    None,
    Pkcs12(Pkcs12),
}

/// Represents wether the options are for a client (connector) or a server (acceptor).
#[derive(Debug, Eq, PartialEq)]
pub enum TlsOptionsType {
    Connector,
    Acceptor,
}

/// A set of options for a TLS connection.
pub struct TlsOptions {
    #[allow(dead_code)]
    pub(crate) tls_version: TlsVersion,
    pub(crate) identity: Identity,
    pub(crate) root_certificates: Vec<Certificate>,
    pub(crate) ty: TlsOptionsType,
}

/// A builder for `TlsOptions`.
pub struct TlsOptionsBuilder {
    tls_version: Option<TlsVersion>,
    pkcs12: Option<Pkcs12>,
    root_certificates: Vec<Certificate>,
}

impl TlsOptionsBuilder {
    /// Get a builder instance.
    pub fn new() -> Self {
        TlsOptionsBuilder {
            tls_version: None,
            pkcs12: None,
            root_certificates: vec![],
        }
    }

    /// Use TLS 1.2.
    pub fn with_tls_1_2(mut self) -> Self {
        self.tls_version = Some(TlsVersion::V12);
        self
    }

    /// Set the peer certificate.
    pub fn cert_from_pkcs12(mut self, der: &[u8], password: &str) -> Self {
        self.pkcs12 = Some(Pkcs12 {
            der: Vec::from(der),
            password: String::from(password),
        });
        self
    }

    /// Add a root certificate.
    ///
    /// This is only used for connectors at the moment.
    pub fn add_root_certificate(mut self, certificate: Certificate) -> Self {
        self.root_certificates.push(certificate);
        self
    }

    /// Get the TlsOptions for a connector.
    pub fn connector(mut self) -> Result<TlsOptions> {
        let identity = match self.pkcs12.take() {
            Some(i) => Identity::Pkcs12(i),
            None => Identity::None,
        };

        Ok(TlsOptions {
            tls_version: self.tls_version.take().unwrap_or(TlsVersion::V12),
            identity,
            root_certificates: self.root_certificates,
            ty: TlsOptionsType::Connector,
        })
    }

    /// Get the TlsOptions for an acceptor.
    pub fn acceptor(self) -> Result<TlsOptions> {
        let mut self_mut = self;

        let pkcs12 = match self_mut.pkcs12.take() {
            Some(i) => i,
            _ => {
                return Err(CowRpcError::Internal(
                    "Missing tls identity, you need to set a certificate".to_string(),
                ))
            }
        };

        Ok(TlsOptions {
            tls_version: self_mut.tls_version.take().unwrap_or(TlsVersion::V12),
            identity: Identity::Pkcs12(pkcs12),
            root_certificates: self_mut.root_certificates,
            ty: TlsOptionsType::Acceptor,
        })
    }
}
