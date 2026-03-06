# dss-parallel-web-plugin
Dataiku plugin providing a dataset connector powered by Parallel FindAll API using the official Python SDK (`parallel-web`).

## Dataset connector
Connector id: `parallel-web-systems_web-dataset-generator`

The connector executes this SDK flow:
1. `client.beta.findall.ingest(objective=...)` to create a FindAll schema.
2. `client.beta.findall.create(run_input)` to start the run.
3. Poll `client.beta.findall.retrieve(findall_id=...)` until completion.
4. Read candidates via `client.beta.findall.result(findall_id=...)`.

## Required settings
- `Parallel API Auth Preset`: choose a preset from parameter set `parallel-api-key`.
- `Objective`: natural language objective for FindAll.

## Optional settings
- `Generator`: one of `preview`, `base`, `core`, `pro`.
- `Match Limit`: number of candidates to request.
- `Include Unmatched`: include candidates whose `match_status` is not `matched`.
- `Include Citations`: include `basis` (citations/reasoning) in rows.
- `Enrichment Fields`: optional structured list of fields (`name`, `type`, `description`); if set, connector runs `findall.enrich(...)` before fetching results using the same processor as `Generator`.
- Polling controls.
