# Dataset sources

The Wikipedia and Stack Exchange benchmarks use prepared UTF-8 CSV files with
an `id,body` header. This document records the upstream inputs, preparation,
query sources, and licensing references for those files.

## English Wikipedia

The corpus comes from [Search Benchmark Game](https://github.com/quickwit-oss/search-benchmark-game),
the search benchmark used by Tantivy. Our import follows revision
[`a7c75473e91746280c5f01e69bf594ece5fca560`](https://github.com/quickwit-oss/search-benchmark-game/tree/a7c75473e91746280c5f01e69bf594ece5fca560).

- Original download: [wiki-articles.json.bz2](https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2?dl=1),
  as specified in the upstream [Makefile](https://github.com/quickwit-oss/search-benchmark-game/blob/a7c75473e91746280c5f01e69bf594ece5fca560/Makefile).
- Upstream text preparation: [corpus_transform.py](https://github.com/quickwit-oss/search-benchmark-game/blob/a7c75473e91746280c5f01e69bf594ece5fca560/corpus_transform.py).
- Upstream query source: [queries.txt](https://github.com/quickwit-oss/search-benchmark-game/blob/a7c75473e91746280c5f01e69bf594ece5fca560/queries.txt).

The prepared corpus contains **5,032,104 documents**. Each source article's URL
becomes its `id`. Its body is lowercased, then each run matching `[^a-zA-Z]+`
is replaced with one space, following the upstream transformation. Document
order and empty bodies are preserved; the resulting text is not trimmed.
Invalid JSON records and empty URLs are skipped as in the upstream script.
Article titles and the upstream random sorting field are omitted.

[Our query file](datasets/wikipedia/queries.json) contains **302 query entries**:
the upstream `term` entry and 301 `union` entries, preserving their text, order,
and intentional repetitions. Each supplies the text for backend-specific
conjunction, disjunction, and phrase forms. The corresponding upstream
intersection and phrase entries were checked against those texts. Mixed
required/optional clauses, negation, and the upstream two-phase critic case
are outside this workload's query forms and were excluded.

The article text is contributed by Wikipedia authors. See
[Wikipedia's content reuse and licensing guidance](https://en.wikipedia.org/wiki/Wikipedia:Reusing_Wikipedia_content)
for its CC BY-SA terms and attribution provisions; the CSV retains the article
URLs. Search Benchmark Game separately publishes an
[MIT license](https://github.com/quickwit-oss/search-benchmark-game/blob/a7c75473e91746280c5f01e69bf594ece5fca560/LICENSE)
for its repository.

The original compressed download's size and SHA-256, upstream revision, and
transformation are recorded in [source.json](datasets/wikipedia/source.json).

## Stack Exchange

The source is the [Stack Exchange Data Dump 2026-06-30](https://archive.org/details/stackexchange_20260630_sakura),
Internet Archive item `stackexchange_20260630_sakura`. This is a community
republication of the Stack Exchange network's XML dumps, containing data
through June 30, 2026. We downloaded and extracted its **362 site archives**
in `.7z` format.

The prepared corpus is a seeded random sample of **150,000,000 nonempty post
and comment bodies** across those sites:

- Posts supply the `Body` attribute from `Posts.xml`.
- Comments supply the `Text` attribute from `Comments.xml`.
- XML parsing decodes XML entities once. Embedded HTML in post bodies remains
  part of the text.
- Sampled records receive sequential IDs from 1 through 150,000,000. Original
  site, post/comment, author, and revision metadata are not included in the CSV.

The complete CSV was validated for record structure, sequential IDs, UTF-8,
and absence of embedded NUL bytes. The prepared file used here preserves that
validated CSV byte for byte; the Makefile does not repeat the sampling step.

Queries originated as sampled runs of consecutive words from the body CSV.
An initial tokenization pass produced the prepared query text. Subsequent
JSON conversion encoded each engine's query syntax without further
tokenization, stemming, or stopword filtering. Empty queries and queries longer
than 15 whitespace-separated terms were removed.

[queries.json](datasets/stackexchange/queries.json) contains **1,254 queries**.

The archives' bundled `license.txt` describes the dump as a whole as
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/), with individual
contributions originating under versions 2.5, 3.0, or 4.0. Stack Exchange's
[official licensing page](https://stackoverflow.com/help/licensing) explains
the contribution-date boundaries and links to the applicable licenses.

The archive URL and the prepared CSV's scope are recorded in
[source.json](datasets/stackexchange/source.json).

## Prepared files and checksums

Each dataset's `data.csv.gz` uses gzip level 9 with the original filename and
timestamp omitted. `make -f Makefile.wikipedia create` and
`make -f Makefile.stackexchange create` decompress the supplied prepared
archive and verify the resulting CSV against `SHA256SUMS`. These prepared
archives and CSVs are excluded from Git.

| Dataset directory | CSV bytes | Gzip bytes | File identities |
| --- | ---: | ---: | --- |
| `datasets/wikipedia/` | 8,093,810,896 | 2,685,744,356 | [Manifest](datasets/wikipedia/data-manifest.json), [SHA-256 checksums](datasets/wikipedia/SHA256SUMS) |
| `datasets/stackexchange/` | 84,521,226,768 | 28,137,896,404 | [Manifest](datasets/stackexchange/data-manifest.json), [SHA-256 checksums](datasets/stackexchange/SHA256SUMS) |

See [the benchmark instructions](benchmarks/README.md) for loading these files
and selecting the query file used by a run.
