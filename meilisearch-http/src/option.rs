use std::fs;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;

use rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls::{
    AllowAnyAnonymousOrAuthenticatedClient, AllowAnyAuthenticatedClient, NoClientAuth,
    RootCertStore,
};
use structopt::StructOpt;

const POSSIBLE_ENV: [&str; 2] = ["development", "production"];

#[derive(Debug, Default, Clone, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created.
    #[structopt(long, env = "MEILI_DB_PATH", default_value = "./data.ms")]
    pub db_path: String,

    /// The address on which the http server will listen.
    #[structopt(long, env = "MEILI_HTTP_ADDR", default_value = "127.0.0.1:7700")]
    pub http_addr: String,

    /// The master key allowing you to do everything on the server.
    #[structopt(long, env = "MEILI_MASTER_KEY")]
    pub master_key: Option<String>,

    /// This environment variable must be set to `production` if your are running in production.
    /// If the server is running in development mode more logs will be displayed,
    /// and the master key can be avoided which implies that there is no security on the updates routes.
    /// This is useful to debug when integrating the engine with another service.
    #[structopt(long, env = "MEILI_ENV", default_value = "development", possible_values = &POSSIBLE_ENV)]
    pub env: String,

    /// Do not send analytics to Meili.
    #[structopt(long, env = "MEILI_NO_ANALYTICS")]
    pub no_analytics: bool,

    /// The maximum size, in bytes, of the main lmdb database directory
    #[structopt(long, env = "MEILI_MAIN_MAP_SIZE", default_value = "107374182400")] // 100GB
    pub main_map_size: usize,

    /// The maximum size, in bytes, of the update lmdb database directory
    #[structopt(long, env = "MEILI_UPDATE_MAP_SIZE", default_value = "107374182400")] // 100GB
    pub update_map_size: usize,

    /// Read server certificates from CERTFILE.
    /// This should contain PEM-format certificates
    /// in the right order (the first certificate should
    /// certify KEYFILE, the last should be a root CA).
    #[structopt(long, env = "MEILI_SSL_CERT_PATH", parse(from_os_str))]
    pub ssl_cert_path: Option<PathBuf>,

    /// Read private key from KEYFILE.  This should be a RSA
    /// private key or PKCS8-encoded private key, in PEM format.
    #[structopt(long, env = "MEILI_SSL_KEY_PATH", parse(from_os_str))]
    pub ssl_key_path: Option<PathBuf>,

    /// Enable client authentication, and accept certificates
    /// signed by those roots provided in CERTFILE.
    #[structopt(long, env = "MEILI_SSL_AUTH_PATH", parse(from_os_str))]
    pub ssl_auth_path: Option<PathBuf>,

    /// Read DER-encoded OCSP response from OCSPFILE and staple to certificate.
    /// Optional
    #[structopt(long, env = "MEILI_SSL_OCSP_PATH", parse(from_os_str))]
    pub ssl_ocsp_path: Option<PathBuf>,

    /// Send a fatal alert if the client does not complete client authentication.
    #[structopt(long, env = "MEILI_SSL_REQUIRE_AUTH")]
    pub ssl_require_auth: bool,

    /// SSL support session resumption
    #[structopt(long, env = "MEILI_SSL_RESUMPTION")]
    pub ssl_resumption: bool,

    /// SSL support tickets.
    #[structopt(long, env = "MEILI_SSL_TICKETS")]
    pub ssl_tickets: bool,
}

impl Opt {
    pub fn get_ssl_config(&self) -> Option<rustls::ServerConfig> {
        if let (Some(cert_path), Some(key_path)) = (&self.ssl_cert_path, &self.ssl_key_path) {
            let client_auth = match &self.ssl_auth_path {
                Some(auth_path) => {
                    let roots = load_certs(auth_path.to_path_buf());
                    let mut client_auth_roots = RootCertStore::empty();
                    for root in roots {
                        client_auth_roots.add(&root).unwrap();
                    }
                    if self.ssl_require_auth {
                        AllowAnyAuthenticatedClient::new(client_auth_roots)
                    } else {
                        AllowAnyAnonymousOrAuthenticatedClient::new(client_auth_roots)
                    }
                }
                None => NoClientAuth::new(),
            };

            let mut config = rustls::ServerConfig::new(client_auth);
            config.key_log = Arc::new(rustls::KeyLogFile::new());

            let certs = load_certs(cert_path.to_path_buf());
            let privkey = load_private_key(key_path.to_path_buf());
            let ocsp = load_ocsp(&self.ssl_ocsp_path);
            config
                .set_single_cert_with_ocsp_and_sct(certs, privkey, ocsp, vec![])
                .expect("bad certificates/private key");

            if self.ssl_resumption {
                config.set_persistence(rustls::ServerSessionMemoryCache::new(256));
            }

            if self.ssl_tickets {
                config.ticketer = rustls::Ticketer::new();
            }

            Some(config)
        } else {
            None
        }
    }
}

fn load_certs(filename: PathBuf) -> Vec<rustls::Certificate> {
    let certfile = fs::File::open(filename).expect("cannot open certificate file");
    let mut reader = BufReader::new(certfile);
    certs(&mut reader).unwrap()
}

fn load_private_key(filename: PathBuf) -> rustls::PrivateKey {
    let rsa_keys = {
        let keyfile = fs::File::open(filename.clone()).expect("cannot open private key file");
        let mut reader = BufReader::new(keyfile);
        rsa_private_keys(&mut reader).expect("file contains invalid rsa private key")
    };

    let pkcs8_keys = {
        let keyfile = fs::File::open(filename).expect("cannot open private key file");
        let mut reader = BufReader::new(keyfile);
        pkcs8_private_keys(&mut reader)
            .expect("file contains invalid pkcs8 private key (encrypted keys not supported)")
    };

    // prefer to load pkcs8 keys
    if !pkcs8_keys.is_empty() {
        pkcs8_keys[0].clone()
    } else {
        assert!(!rsa_keys.is_empty());
        rsa_keys[0].clone()
    }
}

fn load_ocsp(filename: &Option<PathBuf>) -> Vec<u8> {
    let mut ret = Vec::new();

    if let &Some(ref name) = filename {
        fs::File::open(name)
            .expect("cannot open ocsp file")
            .read_to_end(&mut ret)
            .unwrap();
    }

    ret
}
