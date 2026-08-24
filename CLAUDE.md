# amber-store-core — project notes

## Benchmarks

`cmd/amber-bench` is the ingest → delete → gc benchmark; its doc comment
describes the workload and every flag. `go test ./cmd/amber-bench` runs the
whole pipeline at smoke scale (30 refs, 0.1× sizes, 4 MiB packs; ~8 s,
builds the CLI itself). Full scale needs ~100 GiB of disk and a few minutes:

```sh
go build -o /tmp/amber-store ./cmd/amber-store
go run ./cmd/amber-bench -data /tmp/bench/data -store /tmp/bench/store \
    -restore /tmp/bench/restore -bin /tmp/amber-store -out /tmp/bench/results.json
go run ./cmd/amber-bench -phase report -out /tmp/bench/results.json   # reprint
```

The dataset is seeded, so reruns get the identical files and pack layout;
delete the data, store and binary afterwards.

### What it measures

- **Workload.** 1000 references, ~50 GiB unique: 300 "kept" refs of 100 MiB
  fresh random data and 700 "deleted" refs of 30 MiB, interleaved (kept =
  `i%10 < 3`). Every ref also holds whole-file reflinks of ~⅓ of its fresh
  size from the previous ref, so ~25 % of the 66 GiB ingested is duplicate,
  and kept refs share data with deleted neighbours. Files 256 KiB–4 MiB.
- **Ingest rate** (in-process `ingest.Dir` + collector `PrepareRef`, as the
  CLI/daemon do), per-ref timings, dedup counts.
- **Delete** of the 700 refs through `ReleaseRef`.
- **GC**: `amber-store gc run --grace 1s` under policy (0.5 garbage line),
  then a forced `--garbage 0` pass; `gc status` and `du` around each step;
  nominal vs really reclaimed bytes.
- **Integrity**: `fstree.CheckComplete` on every survivor, and a sample of
  kept refs restored through the CLI and diffed against the source.

### Results, 2026-08-25 (branch `simple-gc`)

Apple-silicon Mac, 14 cores, 48 GiB RAM, APFS SSD; 256 MiB segments, fsync
on, default chunking. Dataset: 66.16 GiB ingested, 49.80 GiB unique, 24.7 %
duplicate, 35,128 files. 200 packs after ingest.

| step | sequential copier (f7821fc) | pipelined copier (f739c7d) |
| --- | --- | --- |
| ingest 1000 refs | 120.5 s, 562 MiB/s avg (900 MiB/s steady, then memory-pressure stalls) | 71.3 s, 950 MiB/s avg, 716 MiB/s new bytes |
| ref put (closure walk) | 15 → 24 ms/ref over the run | 15 → 18 ms/ref |
| delete 700 refs | 9.6 s (13.7 ms/ref) | 10.9 s (15.5 ms/ref) |
| `gc run` (0.5 line): 199 scored, 63–64 reaped, 5.6 GiB copied, 16 GiB freed | **18.0 s** | **8.6 s** |
| `gc run --garbage 0`: 106–107 reaped, 19.5 GiB copied, 26.6 GiB freed | **53.5 s** | **33.0 s** |
| copier throughput | ~330–370 MiB/s | ~600–680 MiB/s |
| packstore on disk: ingest → policy → forced | 49.96 → 39.69 → 32.52 GiB | 49.90 → 39.77 → 32.52 GiB |
| integrity | 300/300 complete, 10 restores identical | same |

What "really got deleted": the 700 refs nominally own 20.5 GiB, but 3.2 GiB
of it stays live through the kept refs' copies (the collector reports
garbage 17.3 GiB right after the deletes). The policy run frees ~10.2 GiB
on disk — it deletes 16 GiB of packs but re-appends 5.6 GiB of survivors —
and leaves 7.1 GiB of garbage in the 105 packs under the 50 % line; the
forced pass reclaims the rest, 17.4 GiB in total (85 % of nominal), copying
19.5 GiB to free 7.2 GiB net.

Where the time goes: scoring is parallel and negligible; a cycle is the
copy loop. With the copy pipelined across `Jobs` workers it sits at the
same ceiling as ingest's new-byte rate — appends and `syncActive`'s fsync
both serialize on the packstore's append lock — so the next lever is the
fsync under that lock, not more workers. Delete cost is the union rebuild
per `ReleaseRef`. The first run's ingest tail (stalls up to 3 s on single
refs from ref ~600 on) was macOS memory pressure — 67 GiB of fresh source
plus 50 GiB of mmapped packs against 48 GiB of RAM — not the store; the
rerun did not reproduce it.
