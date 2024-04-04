use ahash::AHashMap;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use axum_msgpack::MsgPack;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::OptsBuilder;
use mysql_async::Pool;
use mysql_async::PoolConstraints;
use mysql_async::PoolOpts;
use mysql_async::Row;
use serde::Deserialize;
use service_toolkit::panic::set_up_panic_hook;
use service_toolkit::server::build_port_server_with_tls;
use service_toolkit::server::TlsCfg;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::read_to_string;
use tracing_subscriber::EnvFilter;

type RmpV = rmpv::Value;
type SqlV = mysql_async::Value;

fn rmpv_to_sqlv(v: impl Into<RmpV>) -> SqlV {
  let v: RmpV = v.into();
  match v {
    RmpV::Nil => SqlV::NULL,
    RmpV::Boolean(v) => SqlV::Int(if v { 1 } else { 0 }),
    RmpV::Integer(v) => SqlV::Int(v.as_i64().unwrap()),
    RmpV::F32(v) => SqlV::Float(v),
    RmpV::F64(v) => SqlV::Double(v),
    RmpV::String(v) => SqlV::Bytes(v.into_bytes()),
    RmpV::Binary(v) => SqlV::Bytes(v),
    RmpV::Array(_) => todo!(),
    RmpV::Map(_) => todo!(),
    RmpV::Ext(typ, raw) => match typ {
      -1 => {
        // https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type
        let (sec, ns) = match raw.len() {
          4 => (u32::from_be_bytes(raw.try_into().unwrap()).into(), 0u64),
          8 => {
            let ns: u64 = (u32::from_be_bytes(raw[..4].try_into().unwrap()) >> 2).into();
            let sec = u64::from_be_bytes(raw.try_into().unwrap())
              & 0b11_11111111_11111111_11111111_11111111;
            (sec, ns)
          }
          12 => {
            let ns: u64 = u32::from_be_bytes(raw[..4].try_into().unwrap()).into();
            let sec = u64::from_be_bytes(raw[4..].try_into().unwrap());
            (sec, ns)
          }
          _ => unreachable!(),
        };
        let ts = Utc
          .timestamp_opt(sec.try_into().unwrap(), ns.try_into().unwrap())
          .unwrap();
        SqlV::Date(
          ts.year().try_into().unwrap(),
          ts.month().try_into().unwrap(),
          ts.day().try_into().unwrap(),
          ts.hour().try_into().unwrap(),
          ts.minute().try_into().unwrap(),
          ts.second().try_into().unwrap(),
          ts.nanosecond() / 1000,
        )
      }
      _ => todo!(),
    },
  }
}

fn sqlv_to_rmpv(v: impl Into<SqlV>) -> RmpV {
  let v: SqlV = v.into();
  match v {
    SqlV::NULL => RmpV::Nil,
    SqlV::Bytes(v) => RmpV::Binary(v),
    SqlV::Int(v) => RmpV::Integer(rmpv::Integer::from(v)),
    SqlV::UInt(v) => RmpV::Integer(rmpv::Integer::from(v)),
    SqlV::Float(v) => RmpV::F32(v),
    SqlV::Double(v) => RmpV::F64(v),
    SqlV::Date(y, mth, d, h, min, s, us) => RmpV::from(
      // I don't know why MariaDB stores `0` as this instead of (1970, 1, 1, 0, 0, 0, 0).
      if (y, mth, d, h, min, s, us) == (0, 0, 0, 0, 0, 0, 0) {
        Utc.timestamp_nanos(0)
      } else {
        Utc
          .with_ymd_and_hms(
            y.into(),
            mth.into(),
            d.into(),
            h.into(),
            min.into(),
            s.into(),
          )
          .unwrap()
          .with_nanosecond(us * 1000)
          .unwrap()
      }
      .to_rfc3339(),
    ),
    SqlV::Time(neg, d, h, m, s, us) => todo!(),
  }
}

struct Ctx {
  db: Pool,
}

#[derive(Deserialize)]
struct QueryInput {
  query: String,
  params: Vec<RmpV>,
}

async fn endpoint_query(
  State(ctx): State<Arc<Ctx>>,
  MsgPack(req): MsgPack<QueryInput>,
) -> Result<MsgPack<Vec<AHashMap<String, RmpV>>>, (StatusCode, String)> {
  let mut db = match ctx.db.get_conn().await {
    Ok(db) => db,
    Err(error) => return Err((StatusCode::INTERNAL_SERVER_ERROR, error.to_string())),
  };
  let res: Vec<Row> = match db
    .exec(
      req.query,
      req.params.into_iter().map(rmpv_to_sqlv).collect_vec(),
    )
    .await
  {
    Ok(res) => res,
    Err(error) => return Err((StatusCode::BAD_REQUEST, error.to_string())),
  };
  let rows: Vec<AHashMap<String, RmpV>> = match res.first() {
    Some(fr) => {
      let cols = fr.columns();
      res
        .into_iter()
        .map(|raw_row| {
          raw_row
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(i, v)| (cols[i].name_str().to_string(), sqlv_to_rmpv(v)))
            .collect()
        })
        .collect()
    }
    None => vec![],
  };
  Ok(MsgPack(rows))
}

#[derive(Deserialize)]
struct ExecInput {
  query: String,
  params: Vec<RmpV>,
}

async fn endpoint_exec(
  State(ctx): State<Arc<Ctx>>,
  MsgPack(req): MsgPack<ExecInput>,
) -> Result<(), (StatusCode, String)> {
  let mut db = match ctx.db.get_conn().await {
    Ok(db) => db,
    Err(error) => return Err((StatusCode::INTERNAL_SERVER_ERROR, error.to_string())),
  };
  if let Err(error) = db
    .exec_drop(
      req.query,
      req.params.into_iter().map(rmpv_to_sqlv).collect_vec(),
    )
    .await
  {
    return Err((StatusCode::BAD_REQUEST, error.to_string()));
  };
  Ok(())
}

#[derive(Deserialize)]
struct BatchInput {
  query: String,
  params: Vec<Vec<RmpV>>,
}

async fn endpoint_batch(
  State(ctx): State<Arc<Ctx>>,
  MsgPack(req): MsgPack<BatchInput>,
) -> Result<(), (StatusCode, String)> {
  let mut db = match ctx.db.get_conn().await {
    Ok(db) => db,
    Err(error) => return Err((StatusCode::INTERNAL_SERVER_ERROR, error.to_string())),
  };
  if let Err(error) = db
    .exec_batch(
      req.query,
      req
        .params
        .into_iter()
        .map(|row| row.into_iter().map(rmpv_to_sqlv).collect_vec())
        .collect_vec(),
    )
    .await
  {
    return Err((StatusCode::BAD_REQUEST, error.to_string()));
  };
  Ok(())
}

#[derive(Deserialize)]
struct Cfg {
  database: String,
  hostname: String,
  password: String,
  port: u16,
  username: String,
  max_pool_connections: Option<usize>,

  server_interface: Ipv4Addr,
  server_port: u16,
  server_ssl_cert: PathBuf,
  server_ssl_key: PathBuf,
  server_ssl_ca: PathBuf,
}

#[tokio::main]
async fn main() {
  set_up_panic_hook();

  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .json()
    .init();

  let cfg: Cfg = toml::from_str(
    &read_to_string("db-rpc.toml")
      .await
      .expect("read config file"),
  )
  .expect("parse config file");

  let opts =
    OptsBuilder::default()
      .ip_or_hostname(cfg.hostname)
      .db_name(Some(cfg.database))
      .user(Some(cfg.username))
      .pass(Some(cfg.password))
      .client_found_rows(false)
      .pool_opts(PoolOpts::new().with_constraints(
        PoolConstraints::new(0, cfg.max_pool_connections.unwrap_or(9500)).unwrap(),
      ))
      .tcp_port(cfg.port);
  let db = Pool::new(opts);

  let ctx = Arc::new(Ctx { db });

  let app = Router::new()
    .route("/healthz", get(|| async {}))
    .route("/batch", post(endpoint_batch))
    .route("/exec", post(endpoint_exec))
    .route("/query", post(endpoint_query))
    .layer(DefaultBodyLimit::max(1024 * 1024 * 128))
    .with_state(ctx.clone());

  build_port_server_with_tls(cfg.server_interface, cfg.server_port, &TlsCfg {
    ca: Some(
      tokio::fs::read(cfg.server_ssl_ca)
        .await
        .expect("read SSL CA file"),
    ),
    key: tokio::fs::read(cfg.server_ssl_key)
      .await
      .expect("read SSL key file"),
    cert: tokio::fs::read(cfg.server_ssl_cert)
      .await
      .expect("read SSL certificate file"),
  })
  .serve(app.into_make_service())
  .await
  .unwrap();
}
