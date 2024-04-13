use percent_encoding::utf8_percent_encode;
use percent_encoding::NON_ALPHANUMERIC;
use reqwest::header::CONTENT_TYPE;
use reqwest::Method;
use rmpv::Value;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::fmt::Display;

#[derive(Debug)]
pub enum DbRpcClientError {
  Api {
    status: u16,
    error: String,
    error_details: Option<String>,
  },
  Unauthorized,
  Request(reqwest::Error),
}

impl Display for DbRpcClientError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      DbRpcClientError::Api {
        status,
        error,
        error_details,
      } => write!(f, "API error ({status} - {error}): {error_details:?}"),
      DbRpcClientError::Unauthorized => write!(f, "unauthorized"),
      DbRpcClientError::Request(e) => write!(f, "request error: {e}"),
    }
  }
}

impl Error for DbRpcClientError {}

pub type DbRpcClientResult<T> = Result<T, DbRpcClientError>;

#[derive(Clone, Debug)]
pub struct DbRpcClientCfg {
  api_key: Option<String>,
  endpoint: String,
}

#[derive(Clone, Debug)]
pub struct DbRpcClient {
  r: reqwest::Client,
  cfg: DbRpcClientCfg,
}

impl DbRpcClient {
  pub fn with_request_client(request_client: reqwest::Client, cfg: DbRpcClientCfg) -> Self {
    Self {
      r: request_client,
      cfg,
    }
  }

  pub fn new(cfg: DbRpcClientCfg) -> Self {
    Self::with_request_client(reqwest::Client::new(), cfg)
  }

  async fn raw_request<I: Serialize, O: DeserializeOwned>(
    &self,
    method: Method,
    path: impl AsRef<str>,
    body: Option<&I>,
  ) -> DbRpcClientResult<O> {
    let mut req = self
      .r
      .request(method, format!("{}{}", self.cfg.endpoint, path.as_ref()))
      .header("accept", "application/msgpack");
    if let Some(k) = &self.cfg.api_key {
      req = req.header("authorization", k);
    };
    if let Some(b) = body {
      let raw = rmp_serde::to_vec_named(b).unwrap();
      req = req.header("content-type", "application/msgpack").body(raw);
    };
    let res = req
      .send()
      .await
      .map_err(|err| DbRpcClientError::Request(err))?;
    let status = res.status().as_u16();
    let res_type = res
      .headers()
      .get(CONTENT_TYPE)
      .and_then(|v| v.to_str().ok().map(|v| v.to_string()))
      .unwrap_or_default();
    let res_body_raw = res
      .bytes()
      .await
      .map_err(|err| DbRpcClientError::Request(err))?;
    if status == 401 {
      return Err(DbRpcClientError::Unauthorized);
    };
    #[derive(Deserialize)]
    struct ApiError {
      error: String,
      error_details: Option<String>,
    }
    if status < 200 || status > 299 || !res_type.starts_with("application/msgpack") {
      // The server may be behind some proxy, LB, etc., so we don't know what the body looks like for sure.
      return Err(match rmp_serde::from_slice::<ApiError>(&res_body_raw) {
        Ok(api_error) => DbRpcClientError::Api {
          status,
          error: api_error.error,
          error_details: api_error.error_details,
        },
        Err(_) => DbRpcClientError::Api {
          status,
          // We don't know if the response contains valid UTF-8 text or not.
          error: String::from_utf8_lossy(&res_body_raw).into_owned(),
          error_details: None,
        },
      });
    };
    Ok(rmp_serde::from_slice(&res_body_raw).unwrap())
  }

  pub fn database(&self, db_name: &str) -> DbRpcDbClient {
    DbRpcDbClient {
      c: self.clone(),
      dbpp: format!("/db/{}", utf8_percent_encode(&db_name, NON_ALPHANUMERIC)),
    }
  }
}

#[derive(Clone, Debug)]
pub struct DbRpcDbClient {
  c: DbRpcClient,
  dbpp: String,
}

#[derive(Deserialize)]
pub struct ExecResult {
  pub affected_rows: u64,
  pub last_insert_id: Option<u64>,
}

impl DbRpcDbClient {
  pub async fn batch(
    &self,
    query: impl AsRef<str>,
    params: Vec<Vec<Value>>,
  ) -> DbRpcClientResult<Vec<ExecResult>> {
    #[derive(Serialize)]
    struct Input<'a> {
      query: &'a str,
      params: Vec<Vec<Value>>,
    }
    self
      .c
      .raw_request(
        Method::POST,
        format!("{}/batch", self.dbpp),
        Some(&Input {
          query: query.as_ref(),
          params,
        }),
      )
      .await
  }

  pub async fn exec(
    &self,
    query: impl AsRef<str>,
    params: Vec<Value>,
  ) -> DbRpcClientResult<ExecResult> {
    #[derive(Serialize)]
    struct Input<'a> {
      query: &'a str,
      params: Vec<Value>,
    }
    self
      .c
      .raw_request(
        Method::POST,
        format!("{}/exec", self.dbpp),
        Some(&Input {
          query: query.as_ref(),
          params,
        }),
      )
      .await
  }

  pub async fn query<R: DeserializeOwned>(
    &self,
    query: impl AsRef<str>,
    params: Vec<Value>,
  ) -> DbRpcClientResult<Vec<R>> {
    #[derive(Serialize)]
    struct Input<'a> {
      query: &'a str,
      params: Vec<Value>,
    }
    self
      .c
      .raw_request(
        Method::POST,
        format!("{}/query", self.dbpp),
        Some(&Input {
          query: query.as_ref(),
          params,
        }),
      )
      .await
  }
}
