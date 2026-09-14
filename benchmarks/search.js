import db from "k6/x/database";
import exec from "k6/execution";
import { Counter } from "k6/metrics";
import { buildRequest, selectBackends, selectQueries } from "./queries.js";

const workload = __ENV.WORKLOAD || "topk";
const style = __ENV.QUERY_STYLE || "disjunction";
const names = selectBackends(
  __ENV.BACKENDS || "paradedb,postgres,elasticsearch",
  workload,
  style,
);
const vus = Number(__ENV.VUS || "1");
const topK = Number(__ENV.TOP_K || "10");
if (!Number.isSafeInteger(vus) || vus < 1)
  throw new Error("VUS must be a positive integer");
const entries = selectQueries(
  JSON.parse(open(__ENV.QUERIES)),
  style,
  Number(__ENV.SEED || "1592614637"),
);
// Encode once at initialization; database analyzers still parse the stored forms.
const requests = Object.fromEntries(
  names.map((name) => [
    name,
    entries.map((entry) => buildRequest(name, workload, entry, topK)),
  ]),
);
const connections = {
  paradedb: `postgres://postgres:postgres@127.0.0.1:${__ENV.PARADEDB_PORT}/benchmark`,
  pg_textsearch: `postgres://postgres:postgres@127.0.0.1:${__ENV.PG_TEXTSEARCH_PORT}/benchmark`,
  postgres: `postgres://postgres:postgres@127.0.0.1:${__ENV.POSTGRES_PORT}/benchmark`,
  elasticsearch: `http://127.0.0.1:${__ENV.ELASTICSEARCH_PORT}`,
};
const backends = db.backends({
  datasetPath: __ENV.CONFIG_DIR,
  backends: names.map((type) => ({
    type,
    connection: connections[type],
    container: `${__ENV.PROJECT}-${type}`,
  })),
});
const phases = db.phases({
  backends: names,
  vus,
  duration: __ENV.DURATION || "60s",
  prewarm: __ENV.PREWARM || "10s",
});
const scenario = (execName, workers) => ({
  executor: "per-vu-iterations",
  vus: workers,
  iterations: 1,
  maxDuration: "168h",
  gracefulStop: "0s",
  exec: execName,
});
const scenarios = Object.fromEntries(
  names.map((name) => [
    name,
    { ...scenario("queryPhase", vus), env: { ACTIVE_BACKEND: name } },
  ]),
);
scenarios.metrics_collector = scenario("collectMetrics", 1);
const queryErrors = new Counter("benchmark_query_errors");
export const options = {
  scenarios,
  thresholds: { benchmark_query_errors: ["count==0"] },
};
backends.enableIndexIOStats();

const prewarmSQL = `WITH relation AS (
  SELECT pg_relation_size($1::text::regclass, 'main') /
         current_setting('block_size')::bigint AS blocks
), chunk AS (
  SELECT blocks * $2::text::bigint / $3::text::bigint AS first_block,
         blocks * ($2::text::bigint + 1) / $3::text::bigint - 1 AS last_block
  FROM relation
)
SELECT pg_prewarm($1::text::regclass, 'buffer', 'main', first_block, last_block)
FROM chunk WHERE first_block <= last_block`;

export function queryPhase() {
  const name = __ENV.ACTIVE_BACKEND;
  const client = backends.get(name);
  const relation =
    name === "postgres" ? "documents_body_gin_idx" : "documents_body_bm25_idx";
  phases.run(
    name,
    name === "elasticsearch" ? 0 : vus,
    (iteration) => {
      const result = client.prewarm(
        prewarmSQL,
        relation,
        String(iteration),
        String(vus),
      );
      if (result?.error) throw new Error(result.error);
    },
    () => {
      backends.resetIndexIOStats([name]);
      backends.resetPostgresDiagnostics([name]);
    },
    (iteration) => {
      const index = Number(iteration) % entries.length;
      exec.vu.metrics.tags.query_id = `${entries[index].record.source_id}:${entries[index].style}`;
      const result = client.query(...requests[name][index]);
      const failed =
        result?.error &&
        result.error !== "benchmark measurement deadline reached";
      queryErrors.add(failed ? 1 : 0, { backend: name });
    },
  );
}

export function collectMetrics() {
  phases.runCollector(
    (name, measuring) => {
      if (measuring) {
        backends.collectPostgresDiagnostics(name, false);
        backends.collect();
      }
    },
    (name, hasNext) => {
      backends.collectPostgresDiagnostics(name, true);
      backends.collectFinal();
      backends.phaseBoundary(name, hasNext ? __ENV.COOLDOWN || "0s" : "0s");
    },
    () => backends.collectFinal(),
  );
}
