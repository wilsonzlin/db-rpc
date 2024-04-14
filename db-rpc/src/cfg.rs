use ahash::AHashMap;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct CfgDb {
  pub api_key: Option<String>,
  pub database: String,
  pub hostname: String,
  pub max_pool_connections: Option<usize>,
  pub password: String,
  pub port: u16,
  pub username: String,
}

#[derive(Deserialize)]
pub(crate) struct CfgServerSsl {
  pub ca: Option<PathBuf>,
  pub cert: PathBuf,
  pub key: PathBuf,
}

#[derive(Deserialize)]
pub(crate) struct CfgServer {
  pub global_api_key: Option<String>,
  pub interface: Ipv4Addr,
  pub max_body_size: Option<usize>,
  pub port: u16,
  pub ssl: Option<CfgServerSsl>,
}

#[derive(Deserialize)]
pub(crate) struct Cfg {
  #[serde(default)]
  pub databases: AHashMap<String, CfgDb>,
  pub server: CfgServer,
}
