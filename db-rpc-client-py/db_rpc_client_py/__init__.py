from dacite import from_dict
from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from urllib.parse import quote
import msgpack
import requests


@dataclass
class QueryChanges:
    affected_rows: int
    last_insert_id: Optional[int]


@dataclass
class DbCfg:
    database: str
    hostname: str
    password: str
    port: int
    username: str
    max_pool_connections: Optional[int]
    api_key: Optional[str]


class DbRpcUnauthorizedError(Exception):
    def __init__(self):
        super().__init__("Authorization failed")


class DbRpcApiError(Exception):
    def __init__(self, status: int, response: Any):
        super().__init__(f"Request to db-rpc failed with status {status}")
        self.status = status
        self.response = response


def dbpp(name: str) -> str:
    return f"/db/{quote(name)}"


class DbRpcDbClient:
    def __init__(self, svc: "DbRpcClient", db: str):
        self.svc = svc
        self.db = db

    def batch(self, query: str, params: List[List[Any]]) -> List[QueryChanges]:
        raw = self.svc.raw_request(
            "POST", f"{dbpp(self.db)}/batch", {"query": query, "params": params}
        )
        return [from_dict(QueryChanges, d) for d in raw]

    def exec(self, query: str, params: List[Any]) -> QueryChanges:
        raw = self.svc.raw_request(
            "POST", f"{dbpp(self.db)}/exec", {"query": query, "params": params}
        )
        return from_dict(QueryChanges, raw)

    def query(self, query: str, params: List[Any]) -> List[Dict[str, Any]]:
        return self.svc.raw_request(
            "POST", f"{dbpp(self.db)}/query", {"query": query, "params": params}
        )


class DbRpcClient:
    def __init__(self, *, endpoint: str, api_key: Optional[str] = None):
        self.endpoint = endpoint
        self.api_key = api_key

    def database(self, db_name: str) -> DbRpcDbClient:
        return DbRpcDbClient(self, db_name)

    def raw_request(
        self, method: str, path: str, body: Optional[Dict[str, Any]]
    ) -> Any:
        url = f"{self.endpoint}{path}"
        res = requests.request(
            method,
            url,
            headers={
                "Authorization": self.api_key,
                "Content-Type": "application/msgpack" if body is not None else None,
            },
            data=msgpack.packb(body) if body is not None else None,
        )
        if res.headers.get("content-type", None) == "application/msgpack":
            res_body = msgpack.unpackb(res.content)
        else:
            res_body = res.text
        if res.status_code == 401:
            raise DbRpcUnauthorizedError()
        elif res.status_code < 200 or res.status_code > 299:
            raise DbRpcApiError(res.status_code, res_body)
        return res_body

    def set_database(self, db_name: str, cfg: DbCfg):
        self.raw_request("PUT", dbpp(db_name), asdict(cfg))

    def delete_database(self, db_name: str):
        self.raw_request("DELETE", dbpp(db_name), None)

    def list_databases(self) -> Dict[str, Any]:
        raw = self.raw_request("GET", "/dbs", None)
        return {db_name: from_dict(DbCfg, cfg) for db_name, cfg in raw.items()}
