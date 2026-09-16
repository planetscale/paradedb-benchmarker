import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  buildRequest,
  parseUpdatesPerSecond,
  selectBackends,
  selectQueries,
} from "./queries.js";

const load = (dataset) =>
  JSON.parse(
    readFileSync(
      new URL(`../datasets/${dataset}/queries.json`, import.meta.url),
      "utf8",
    ),
  );

test("paced updates default to zero and reject invalid rates", () => {
  assert.equal(parseUpdatesPerSecond(), 0);
  assert.equal(parseUpdatesPerSecond("0"), 0);
  assert.equal(parseUpdatesPerSecond("10"), 10);
  assert.equal(parseUpdatesPerSecond("1000000000"), 1000000000);
  for (const value of ["", "-1", "1.5", "NaN", "Infinity", "1e2", "1000000001"]) {
    assert.throws(() => parseUpdatesPerSecond(value), /UPDATES_PER_SECOND/);
  }
});

test("paced updates require supported backends without changing query-only selection", () => {
  const all = "paradedb,pg_textsearch,postgres,elasticsearch";
  assert.deepEqual(selectBackends(all, "topk", "disjunction"), all.split(","));
  assert.deepEqual(selectBackends(all, "topk", "disjunction", 0), all.split(","));
  const postgresBackends = "paradedb,pg_textsearch,postgres";
  assert.deepEqual(
    selectBackends(postgresBackends, "topk", "disjunction", 10),
    postgresBackends.split(","),
  );
  assert.deepEqual(selectBackends("paradedb,postgres", "count", "mixed", 10), [
    "paradedb", "postgres",
  ]);
  assert.throws(() => selectBackends(all, "topk", "disjunction", 10), /elasticsearch/);
  assert.throws(() => selectBackends("pg_textsearch", "count", "mixed", 10), /only WORKLOAD=topk/);
});

test("query traces retain their selected record counts and supported engine forms", () => {
  for (const [dataset, expected] of [
    ["wikipedia", 302],
    ["stackexchange", 1254],
  ]) {
    const contents = load(dataset);
    assert.equal(contents.queries.length, expected);
    for (const record of contents.queries) {
      assert.deepEqual(Object.keys(record.engines), [
        "paradedb",
        "pg_textsearch",
        "postgres",
        "elasticsearch",
      ]);
    }
    for (const workload of ["topk", "count"]) {
      for (const entry of selectQueries(contents, "mixed", 123)) {
        for (const backend of ["paradedb", "postgres", "elasticsearch"]) {
          assert.doesNotThrow(() => buildRequest(backend, workload, entry, 10));
        }
      }
    }
    for (const entry of selectQueries(contents, "disjunction", 123)) {
      const [, value] = buildRequest("pg_textsearch", "topk", entry, 10);
      assert.equal(value, entry.record.engines.pg_textsearch.disjunction);
    }
  }
});

test("mixed styles preserve identity even when single-term query strings coincide", () => {
  const contents = { queries: [load("wikipedia").queries[0]] };
  const entries = selectQueries(contents, "mixed", 1);
  assert.deepEqual(entries.map((entry) => entry.style).sort(), [
    "conjunction",
    "disjunction",
    "phrase",
  ]);
  for (const entry of entries) {
    const [sql, value] = buildRequest("paradedb", "topk", entry, 7);
    assert.match(sql, /LIMIT 7$/);
    assert.equal(
      value,
      contents.queries[0].engines.paradedb[
        entry.style === "disjunction" ? "match" : entry.style
      ],
    );
    assert.equal(sql.includes("pdb.match"), entry.style === "disjunction");
  }
});

test("seeded traversal is repeatable and retains each record/style exactly once", () => {
  const contents = load("wikipedia");
  const ids = (seed) =>
    selectQueries(contents, "mixed", seed).map(
      ({ record, style }) => `${record.source_id}:${style}`,
    );
  assert.deepEqual(ids(123), ids(123));
  assert.notDeepEqual(ids(123), ids(124));
  assert.equal(new Set(ids(123)).size, contents.queries.length * 3);
});

test("unsupported workloads and incomplete forms fail rather than changing query semantics", () => {
  assert.throws(
    () => selectBackends("pg_textsearch", "count", "disjunction"),
    /only WORKLOAD=topk/,
  );
  assert.throws(
    () => selectBackends("pg_textsearch", "topk", "mixed"),
    /only WORKLOAD=topk/,
  );
  assert.throws(
    () => selectBackends("postgres,postgres", "topk", "phrase"),
    /distinct/,
  );
  assert.throws(
    () => selectBackends("missing", "topk", "phrase"),
    /Unknown backend/,
  );
  assert.throws(() => selectQueries(load("wikipedia"), "mixed", -1), /SEED/);
  assert.throws(
    () =>
      buildRequest(
        "postgres",
        "count",
        { record: { source_id: 1 }, style: "phrase" },
        10,
      ),
    /Missing postgres\/phrase/,
  );
});

test("count requests use each engine's matching operation and exact Elasticsearch hit counts", () => {
  const entry = selectQueries(load("wikipedia"), "disjunction", 1)[0];
  assert.match(
    buildRequest("paradedb", "count", entry, 10)[0],
    /count\(\*\).*pdb\.parse/,
  );
  assert.match(
    buildRequest("postgres", "count", entry, 10)[0],
    /count\(\*\).*to_tsquery\('simple'/,
  );
  const [index, body] = buildRequest("elasticsearch", "count", entry, 10);
  assert.equal(index, "documents");
  assert.equal(body.size, 0);
  assert.equal(body.track_total_hits, true);
  assert.equal(
    body.query.query_string.query,
    entry.record.engines.elasticsearch.disjunction,
  );
});
