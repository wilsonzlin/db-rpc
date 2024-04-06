mod cfg;

use ahash::AHashMap;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use axum::Router;
use axum_msgpack::MsgPack;
use cfg::Cfg;
use cfg::CfgDb;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use dashmap::DashMap;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::OptsBuilder;
use mysql_async::Pool;
use mysql_async::PoolConstraints;
use mysql_async::PoolOpts;
use mysql_async::Row;
use serde::Deserialize;
use service_toolkit::panic::set_up_panic_hook;
use service_toolkit::server::build_port_server;
use service_toolkit::server::build_port_server_with_tls;
use service_toolkit::server::TlsCfg;
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

fn build_db_pool_from_cfg(cfg: CfgDb) -> Pool {
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
  Pool::new(opts)
}

struct Db {
  pool: Pool,
  cfg: CfgDb,
}

struct Ctx {
  dbs: DashMap<String, Db>,
  global_api_key: Option<String>,
}

impl Ctx {
  pub(crate) async fn db_conn(
    &self,
    headers: &HeaderMap,
    db_name: impl AsRef<str>,
  ) -> Result<mysql_async::Conn, (StatusCode, String)> {
    let db_name = db_name.as_ref();
    let Some(db) = self.dbs.get(db_name) else {
      return Err((StatusCode::NOT_FOUND, format!("`{db_name}` does not exist")));
    };
    if let Some(expected_api_key) = &db.cfg.api_key {
      let provided_api_key = headers.get("authorization").and_then(|h| h.to_str().ok());
      if !provided_api_key.is_some_and(|k| k == expected_api_key) {
        return Err((StatusCode::UNAUTHORIZED, format!("invalid API key")));
      };
    };
    match db.pool.get_conn().await {
      Ok(db) => Ok(db),
      Err(error) => return Err((StatusCode::INTERNAL_SERVER_ERROR, error.to_string())),
    }
  }

  pub(crate) fn verify_global_auth(&self, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if let Some(expected_api_key) = &self.global_api_key {
      let provided_api_key = headers.get("authorization").and_then(|h| h.to_str().ok());
      if !provided_api_key.is_some_and(|k| k == expected_api_key) {
        return Err((StatusCode::UNAUTHORIZED, format!("invalid API key")));
      };
    };
    Ok(())
  }
}

async fn endpoint_list_dbs(
  State(ctx): State<Arc<Ctx>>,
  headers: HeaderMap,
) -> Result<MsgPack<AHashMap<String, CfgDb>>, (StatusCode, String)> {
  ctx.verify_global_auth(&headers)?;
  Ok(MsgPack(
    ctx
      .dbs
      .iter()
      .map(|e| (e.key().clone(), e.value().cfg.clone()))
      .collect(),
  ))
}

async fn endpoint_put_db(
  State(ctx): State<Arc<Ctx>>,
  Path(db_name): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<CfgDb>,
) -> Result<(), (StatusCode, String)> {
  ctx.verify_global_auth(&headers)?;
  ctx.dbs.insert(db_name, Db {
    pool: build_db_pool_from_cfg(req.clone()),
    cfg: req,
  });
  Ok(())
}

async fn endpoint_delete_db(
  State(ctx): State<Arc<Ctx>>,
  Path(db_name): Path<String>,
  headers: HeaderMap,
) -> Result<(), (StatusCode, String)> {
  ctx.verify_global_auth(&headers)?;
  ctx.dbs.remove(&db_name);
  Ok(())
}

#[derive(Deserialize)]
struct QueryInput {
  query: String,
  params: Vec<RmpV>,
}

async fn endpoint_query(
  State(ctx): State<Arc<Ctx>>,
  Path(db_name): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<QueryInput>,
) -> Result<MsgPack<Vec<AHashMap<String, RmpV>>>, (StatusCode, String)> {
  let mut db = ctx.db_conn(&headers, &db_name).await?;
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
  Path(db_name): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<ExecInput>,
) -> Result<(), (StatusCode, String)> {
  let mut db = ctx.db_conn(&headers, &db_name).await?;
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
  Path(db_name): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<BatchInput>,
) -> Result<(), (StatusCode, String)> {
  let mut db = ctx.db_conn(&headers, &db_name).await?;
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

  let dbs = cfg
    .databases
    .into_iter()
    .map(|(db_name, cfg)| {
      (db_name, Db {
        pool: build_db_pool_from_cfg(cfg.clone()),
        cfg,
      })
    })
    .collect::<DashMap<_, _>>();

  let ctx = Arc::new(Ctx {
    dbs,
    global_api_key: cfg.server.global_api_key,
  });

  let app = Router::new()
    .route("/healthz", get(|| async { env!("CARGO_PKG_VERSION") }))
    .route("/dbs", get(endpoint_list_dbs))
    .route("/db/:db", put(endpoint_put_db).delete(endpoint_delete_db))
    .route("/db/:db/batch", post(endpoint_batch))
    .route("/db/:db/exec", post(endpoint_exec))
    .route("/db/:db/query", post(endpoint_query))
    .layer(DefaultBodyLimit::max(1024 * 1024 * 128))
    .with_state(ctx.clone());

  match cfg.server.ssl {
    Some(ssl) => {
      build_port_server_with_tls(cfg.server.interface, cfg.server.port, &TlsCfg {
        ca: ssl
          .ca
          .map(|ca| std::fs::read(ca).expect("read SSL CA file")),
        key: std::fs::read(ssl.key).expect("read SSL key file"),
        cert: std::fs::read(ssl.cert).expect("read SSL certificate file"),
      })
      .serve(app.into_make_service())
      .await
      .unwrap();
    }
    None => {
      build_port_server(cfg.server.interface, cfg.server.port)
        .serve(app.into_make_service())
        .await
        .unwrap();
    }
  };
}
