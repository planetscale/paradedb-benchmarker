export const backendNames = [
  "paradedb",
  "pg_textsearch",
  "postgres",
  "elasticsearch",
];
export const queryStyles = ["conjunction", "disjunction", "phrase"];

export function parseUpdatesPerSecond(value = "0") {
  const rate = Number(value);
  if (!/^[0-9]{1,10}$/.test(String(value)) || rate > 1000000000) {
    throw new Error("UPDATES_PER_SECOND must be an integer between 0 and 1000000000");
  }
  return rate;
}

export function selectBackends(value, workload, style, updatesPerSecond = 0) {
  const rate = parseUpdatesPerSecond(updatesPerSecond);
  if (!["topk", "count"].includes(workload)) {
    throw new Error("WORKLOAD must be topk or count");
  }
  if (![...queryStyles, "mixed"].includes(style)) {
    throw new Error(
      "QUERY_STYLE must be conjunction, disjunction, phrase, or mixed",
    );
  }
  const names = value.split(/[\s,]+/).filter(Boolean);
  if (names.length === 0 || new Set(names).size !== names.length) {
    throw new Error("BACKENDS must contain distinct backend names");
  }
  for (const name of names) {
    if (!backendNames.includes(name))
      throw new Error(`Unknown backend: ${name}`);
    if (name === "elasticsearch" && rate > 0) {
      throw new Error(
        "elasticsearch does not support paced random updates; select PostgreSQL backends or use UPDATES_PER_SECOND=0",
      );
    }
    if (
      name === "pg_textsearch" &&
      (workload !== "topk" || style !== "disjunction")
    ) {
      throw new Error(
        "pg_textsearch supports only WORKLOAD=topk QUERY_STYLE=disjunction",
      );
    }
  }
  return names;
}

export function selectQueries(contents, style, seed) {
  if (!Array.isArray(contents.queries) || contents.queries.length === 0) {
    throw new Error("The query file must contain a nonempty queries array");
  }
  if (![...queryStyles, "mixed"].includes(style)) {
    throw new Error(`Unknown query style: ${style}`);
  }
  if (!Number.isInteger(seed) || seed < 0 || seed > 0xffffffff) {
    throw new Error("SEED must be an integer between 0 and 4294967295");
  }
  const styles = style === "mixed" ? queryStyles : [style];
  const entries = contents.queries.flatMap((record) =>
    styles.map((queryStyle) => ({ record, style: queryStyle })),
  );
  // Keep records and styles together: single-term forms can have identical text.
  let state = seed >>> 0;
  for (let i = entries.length - 1; i > 0; i--) {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    const j = Math.floor((state / 0x100000000) * (i + 1));
    [entries[i], entries[j]] = [entries[j], entries[i]];
  }
  return entries;
}

function argument(entry, backend, form = entry.style) {
  const value = entry.record.engines?.[backend]?.[form];
  if (typeof value !== "string") {
    throw new Error(
      `Missing ${backend}/${form} query for source ${entry.record.source_id}`,
    );
  }
  return value;
}

export function buildRequest(backend, workload, entry, topK) {
  if (!Number.isInteger(topK) || topK < 1)
    throw new Error("TOP_K must be a positive integer");
  selectBackends(backend, workload, entry.style);
  const count = workload === "count";
  if (backend === "elasticsearch") {
    return [
      "documents",
      {
        query: {
          query_string: {
            query: argument(entry, backend),
            default_field: "body",
            default_operator: "AND",
          },
        },
        size: count ? 0 : topK,
        ...(count ? {} : { _source: ["id", "body"] }),
        track_total_hits: count,
      },
    ];
  }
  if (backend === "paradedb") {
    const match = !count && entry.style === "disjunction";
    const predicate = match
      ? "pdb.match($1)"
      : "pdb.parse($1, lenient => true)";
    const fields = count ? "count(*)" : "id, body, pdb.score(id) AS score";
    return [
      `SELECT ${fields} FROM documents WHERE body @@@ ${predicate}${count ? "" : ` ORDER BY score DESC LIMIT ${topK}`}`,
      argument(entry, backend, match ? "match" : entry.style),
    ];
  }
  if (backend === "postgres") {
    const fields = count
      ? "count(*)"
      : "id, body, ts_rank_cd(body_tsv, to_tsquery('simple', $1)) AS score";
    return [
      `SELECT ${fields} FROM documents WHERE body_tsv @@ to_tsquery('simple', $1)${count ? "" : ` ORDER BY score DESC LIMIT ${topK}`}`,
      argument(entry, backend),
    ];
  }
  return [
    `SELECT id, body, body <@> to_bm25query($1, 'documents_body_bm25_idx') AS score FROM documents ORDER BY score ASC LIMIT ${topK}`,
    argument(entry, backend),
  ];
}
