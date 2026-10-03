# Pre-split clustering: cutting around a scattered commit

## The gap

The pre-split used to cut at the batch's outer edges and at the cold gaps transaction clustering finds. A
commit scattered across a whole partition has neither: no slack at its edges, and its transaction range
covers every bin, so the clusterer sees the partition as hot throughout. The piece merged whole - 500k rows
rewritten to place 500.

## The rule

Cut at an incoming row's own timestamp and that row belongs to neither half - a piece's bounds describe the
rows it holds - so it is written as a `NEW_PIECE` at the tail and the existing rows around it are never read.
A row sharing a timestamp with an existing one works the same way while dedup is off, since pieces are then
allowed to touch (the tie rule in `O3CompositeMergeStrategy.computeActions`).

Cutting at *every* incoming row is not free: 500 rows becomes ~1000 pieces, each new one a single row, and
every later scan pays a page frame per piece. So the rows are clustered first.

## Clustering

`O3CompositeMergeStrategy.computeCuts` walks the batch inside a piece and breaks it into clusters: two
incoming rows belong to different clusters when the existing rows between them outweigh the piece a cut
costs. Each cluster gets a cut at its first row and one just above its last, so it lands in the gap between
two spared pieces. A cluster's own span may hold almost nothing - that is the point.

Rows are apportioned by the piece's uniform density, `rowsBelow()`, so the walk costs one comparison per
incoming row and no I/O.

## Three thresholds, three jobs

| number | meaning |
|---|---|
| `minPieceRows` = 2x `cairo.partition.compaction.avg.rows.piece.lim` | a piece may exist |
| 2x `minPieceRows` | a gap is worth a cut - the cost of a piece on each side of a cluster |
| `liveRows / avg.rows.piece.lim` | pieces the partition tolerates before compaction rewrites it |

Each cut needs its own gap, so cuts cannot exceed the third number - the piece budget bounds itself, and
`cairo.o3.partition.presplit.max.cuts` is only the safety valve above it.

Outcomes: rows clustered in the first and last second -> 2 clusters, 4 cuts. 500 rows scattered over 500k ->
gaps of ~1000 rows against a 64-row bar, so each row is carved out. The same 500 over 50k -> gaps of ~100
rows; still cut. Raise the limit past the gap and it merges whole, which is then the cheaper of the two.

## Bursty data

Every number above comes from a uniform-density estimate, so a piece's "1000 rows between two incoming rows"
can really be one row or a million.

The estimate never decides what gets published. Each cut carries the rows it must leave below and above it,
and `O3PartitionJob.applyCutResolved` already binary-searches the real timestamp column; a cut that spares
less than it promised is dropped there, exactly as one resolving to a piece edge already was. Applying the
cuts highest-first puts the rows a cut spares in front of it: the rows above a cut are bounded by the cut
already applied, so what the check reads is the real gap. The estimate only decides *whether* to look and
roughly *where*.
