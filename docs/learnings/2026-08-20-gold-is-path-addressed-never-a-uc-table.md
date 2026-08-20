ts: 2026-08-20T17:53:50Z
commit: 1d998c6
session: b76699b6-4fa0-4a56-9e48-8f033cb3de6f (pick-up of VANTAGE + PARALLAX; README revamp)
status: verified

fact: Gold is published as a **path-addressed** Delta table on a UC *volume* and is never
registered as a Unity Catalog table. `pit.util.DeltaIO` addresses it as ``delta.`$path` ``
throughout (SQL `MERGE INTO` / `DESCRIBE HISTORY`), there is no `saveAsTable` and no
`CREATE TABLE` anywhere in `src/` or `transform/`, and `databricks.yml` declares volumes
for jars, artifacts and the lake root with no `catalog:`/`schema:` key at all. The volume
paths make it *look* UC-resident, which is what makes this non-obvious: UC governs the
volume, not a table object. Consequence, and the reason it was probed: a consumer that
wants to read gold **through the catalog** has nothing to bind to, so PARALLAX's Unity
Catalog lane is blocked producer-side. That lane's note previously named a different
blocker ("commit `databricks.yml`; land `.gitattributes`") which had already been
satisfied at `fc20a5b` (2026-07-19) and `3bcc0de` (2026-06-28) — wrong when written, not
stale; corrected in the sibling repo's STATUS.md on the strength of this probe.

basis:
```
$ git rev-parse --short HEAD
1d998c6

$ git grep -nE 'saveAsTable|CREATE TABLE|CREATE OR REPLACE TABLE' -- src transform
(no matches)

$ git grep -nE 'delta\.`' -- src | head -4
src/main/scala/pit/util/DeltaIO.scala:25:      .sql(s"DESCRIBE HISTORY delta.`$path` LIMIT 1")
src/main/scala/pit/util/DeltaIO.scala:55:        s"""MERGE INTO delta.`$path` t

$ grep -nE '^\s*(catalog|schema):' databricks.yml
(no catalog/schema key)
```

re-verify: `git grep -nE 'saveAsTable|CREATE TABLE' -- src transform; grep -nE '^\s*(catalog|schema):' databricks.yml` — both must return nothing; any hit means gold has since been registered as a catalog object and PARALLAX's lane-2 blocker has lifted.
