import { decode, encode } from "@msgpack/msgpack";
import {
  VArray,
  VInteger,
  VObjectMap,
  VOptional,
  VString,
  VStruct,
  Validator,
} from "@wzlin/valid";
import asyncTimeout from "@xtjs/lib/js/asyncTimeout";
import bufferToUint8Array from "@xtjs/lib/js/bufferToUint8Array";
import decodeUtf8 from "@xtjs/lib/js/decodeUtf8";
import mapExists from "@xtjs/lib/js/mapExists";
import {
  ClientHttp2Session,
  IncomingHttpHeaders,
  IncomingHttpStatusHeader,
  connect,
} from "node:http2";

export class DbRpcUnauthorizedError extends Error {
  constructor() {
    super("Authorization failed");
  }
}

export class DbRpcApiError extends Error {
  constructor(
    readonly status: number,
    readonly error: string | undefined,
    readonly errorDetails: any | undefined,
  ) {
    super(
      `Request to db-rpc failed with status ${status}: ${error} ${JSON.stringify(errorDetails, null, 2) ?? ""}`,
    );
  }
}

// DB path prefix.
const dbpp = (name: string) => `/db/${encodeURIComponent(name)}`;

export type MsgPackValue =
  | null
  | undefined
  | boolean
  | number
  | string
  | Date
  | ArrayBufferView
  | ReadonlyArray<MsgPackValue>
  | {
      readonly [k: string | number]: MsgPackValue;
    };

export class DbRpcDbClient {
  constructor(
    private readonly svc: DbRpcClient,
    private readonly db: string,
  ) {}

  private get dbpp() {
    return dbpp(this.db);
  }

  async batch(query: string, params: Array<Array<MsgPackValue>>) {
    const res = await this.svc.rawRequest("POST", `${this.dbpp}/batch`, {
      query,
      params,
    });
    const p = new VArray(
      new VStruct({
        affected_rows: new VInteger(),
        last_insert_id: new VOptional(new VInteger()),
      }),
    ).parseRoot(res);
    return p.map((r) => ({
      affectedRows: r.affected_rows,
      lastInsertId: r.last_insert_id,
    }));
  }

  async exec(query: string, params: Array<MsgPackValue>) {
    const res = await this.svc.rawRequest("POST", `${this.dbpp}/exec`, {
      query,
      params,
    });
    const p = new VStruct({
      affected_rows: new VInteger(),
      last_insert_id: new VOptional(new VInteger()),
    }).parseRoot(res);
    return {
      affectedRows: p.affected_rows,
      lastInsertId: p.last_insert_id,
    };
  }

  async query<R>(
    query: string,
    params: Array<MsgPackValue>,
    rowValidator: Validator<R>,
  ) {
    const res = await this.svc.rawRequest("POST", `${this.dbpp}/query`, {
      query,
      params,
    });
    return new VArray(rowValidator).parseRoot(res);
  }
}

export class DbRpcClient {
  // Only HTTP/2 is supported, because multiplexing is extremely beneficial and one-connection-per-query (i.e. HTTP/1.1) will reintroduce a lot of the pain that db-rpc was designed to mitigate in the first place.
  private client: ClientHttp2Session | undefined;

  constructor(
    private readonly opts: {
      apiKey?: string;
      endpoint: string;
      maxRetries?: number;
      ssl?: {
        key?: string;
        cert?: string;
        ca?: string;
        servername?: string;
        rejectUnauthorized?: boolean;
      };
    },
  ) {}

  database(dbName: string) {
    return new DbRpcDbClient(this, dbName);
  }

  async rawRequest(method: string, path: string, body: any) {
    // Construct a URL to ensure it is correct. If it throws, we don't want to retry.
    const reqUrl = new URL(`${this.opts.endpoint}${path}`);
    const reqBody = mapExists(body, encode);
    const { maxRetries = 1 } = this.opts;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        if (!this.client || this.client.closed || this.client.destroyed) {
          this.client = connect(reqUrl, {
            ca: this.opts.ssl?.ca,
            cert: this.opts.ssl?.cert,
            key: this.opts.ssl?.key,
            servername: this.opts.ssl?.servername,
            rejectUnauthorized: this.opts.ssl?.rejectUnauthorized,
            // These settings significantly improve throughput, especially for large payloads and/or long pipes.
            maxSessionMemory: 4096,
            peerMaxConcurrentStreams: 131072,
            settings: {
              initialWindowSize: 1024 * 1024 * 1024,
              maxFrameSize: 2 ** 24 - 1,
            },
          });
        }
        const req = this.client!.request({
          ":method": method,
          ":path": reqUrl.pathname + reqUrl.search,
          Authorization: this.opts.apiKey,
          "Content-Type": mapExists(body, () => "application/msgpack"),
        });
        const resHeaders = await new Promise<
          IncomingHttpHeaders & IncomingHttpStatusHeader
        >((resolve, reject) => {
          req.on("error", reject).on("response", resolve).end(reqBody);
        });
        const resBodyRaw = await new Promise<Buffer>((resolve, reject) => {
          const chunks = Array<Buffer>();
          req
            .on("error", reject)
            .on("data", (c) => chunks.push(c))
            .on("end", () => resolve(Buffer.concat(chunks)));
        });
        const resStatus = resHeaders[":status"]!;
        if (resStatus === 401) {
          throw new DbRpcUnauthorizedError();
        }
        const resType = resHeaders["content-type"] ?? "";
        const resBody: any = /^application\/(x-)?msgpack$/.test(resType)
          ? // It appears that if Buffer is passed to msgpack.decode, it will parse all bytes as Buffer, but if not, it will use Uint8Array. We want Uint8Array values for all bytes.
            decode(bufferToUint8Array(resBodyRaw))
          : decodeUtf8(resBodyRaw);
        if (resStatus < 200 || resStatus > 299) {
          throw new DbRpcApiError(
            resStatus,
            resBody?.error ?? resBody,
            resBody?.error_details ?? undefined,
          );
        }
        return resBody;
      } catch (err) {
        if (
          attempt === maxRetries ||
          err instanceof DbRpcUnauthorizedError ||
          (err instanceof DbRpcApiError && err.status < 500)
        ) {
          throw err;
        }
        await asyncTimeout(
          Math.random() * Math.min(1000 * 60 * 10, 2 ** attempt),
        );
      }
    }
  }

  async setDatabase(
    dbName: string,
    cfg: {
      database: string;
      hostname: string;
      password: string;
      port: number;
      username: string;
      maxPoolConnections?: number;
      apiKey?: string;
    },
  ) {
    await this.rawRequest("PUT", dbpp(dbName), {
      database: cfg.database,
      hostname: cfg.hostname,
      password: cfg.password,
      port: cfg.port,
      username: cfg.username,
      max_pool_connections: cfg.maxPoolConnections,
      api_key: cfg.apiKey,
    });
  }

  async deleteDatabase(dbName: string) {
    await this.rawRequest("DELETE", dbpp(dbName), undefined);
  }

  async listDatabases() {
    const raw = await this.rawRequest("GET", "/dbs", undefined);
    const p = new VObjectMap(
      new VStruct({
        database: new VString(),
        hostname: new VString(),
        password: new VString(),
        port: new VInteger(1, 65535),
        username: new VString(),
        max_pool_connections: new VOptional(new VInteger(0)),
        api_key: new VOptional(new VString()),
      }),
    ).parseRoot(raw);
    return Object.fromEntries(
      Object.entries(p).map(([dbName, cfg]) => [
        dbName,
        {
          database: cfg.database,
          hostname: cfg.hostname,
          password: cfg.password,
          port: cfg.port,
          username: cfg.username,
          maxPoolConnections: cfg.max_pool_connections,
          apiKey: cfg.api_key,
        },
      ]),
    );
  }
}
