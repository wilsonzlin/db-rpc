use ahash::AHashMap;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct CfgDb {
  pub database: String,
  pub hostname: String,
  pub password: String,
  pub port: u16,
  pub username: String,
  pub max_pool_connections: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct CfgServerSsl {
  pub cert: PathBuf,
  pub key: PathBuf,
  pub ca: Option<PathBuf>,
}

#[derive(Deserialize)]
pub(crate) struct CfgServer {
  pub interface: Ipv4Addr,
  pub port: u16,
  pub ssl: Option<CfgServerSsl>,
}

#[derive(Deserialize)]
pub(crate) struct Cfg {
  pub databases: AHashMap<String, CfgDb>,
  pub server: CfgServer,
}