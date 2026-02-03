# Prediction Sync Job

## Overview
`PredictionSync` builds daily prediction snapshots for live-series cards. The job:
- pulls live-series cards from the most recent game year,
- builds inference features that mirror training (`training_data.py` + Colab pipeline),
- runs attribute-level models (hit + pit),
- runs overall OVR models twice (field_on + field_off),
- stores results as immutable prediction runs and per-card predictions so the frontend can show trends over time.

This is designed to be re-runnable; each execution writes a new `PredictionRun` and new `CardPrediction` rows (no upsert).

## Entry Point
- Job class: `apps/backend/src/jobs/prediction_sync.py`
- Invoked via: `python -m src.run_job prediction_sync`

## High-Level Flow
1. **Select cards**
   - Find max `Card.year` and load all `Card.is_live_set == True` for that year.

2. **Define inference window**
   - Use the most recent `RosterUpdate` as the “previous update”.
   - “Today” is `datetime.utcnow()` (naive) and becomes the inference `update_date`.

3. **Feature build (match training)**
   - For each card, build a base row with card metadata and old attribute values.
   - Aggregate MLB stats for the same scopes used in training:
     - `szn_`: season-to-date up to `update_date`
     - `m1_`: last 30 days up to `update_date`
     - `since_`: from last update (or season start if none)
   - Batting, pitching, baserunning, and fielding aggregates match `training_data.py`.
   - Derived features match training (height/weight, position flags, age buckets).

4. **Role split**
   - Split the dataset into pitchers, batters, and two-way using the same position logic as Colab.

5. **Pruning + league-shrunk rate features**
   - Drop split columns and sparse columns exactly as in Colab.
   - Add league-shrunk rate features for batting and pitching using the same windows/splits and priors.

6. **Attribute model inference**
   - For each model under `apps/backend/models/attr_models/<ATTR>/`:
     - load `best_model.json`, `feature_cols.json`, `final_model.joblib`
     - align columns to `feature_cols.json`
     - predict attribute values
   - Hit models run on batters; pit models run on pitchers; two-way cards get both.

7. **OVR model inference (field_on + field_off)**
   - Models are under `apps/backend/models/ovr_models/{hit|pit}/{field_on|field_off}`.
   - Build inputs from:
     - `old_ovr`
     - predicted attribute values (`pred_..._new`)
   - For `field_off`, fielding/run attributes are forced to their old values:
     - `SPD`, `STEAL`, `ARM`, `ACC`, `FLD`, `REAC`, `BLK`
   - Pitchers and hitters each get:
     - field_on OVR
     - field_off OVR
   - Two-way players get both hit + pit predictions, and we average them per mode:
     - `avg(field_on_hit, field_on_pit)`
     - `avg(field_off_hit, field_off_pit)`

8. **Persistence**
   - Create two `PredictionRun` rows:
     - `scope=standard` (field_off)
     - `scope=fielding` (field_on)
   - Insert a `CardPrediction` for each card in each run.
   - `predicted_attributes` stores the model inputs used for OVR (prefixed with `hit_` / `pit_`).

## Key Files
- Job implementation: `apps/backend/src/jobs/prediction_sync.py`
- Training feature reference: `apps/backend/src/scripts/training_data.py`
- Training/inference logic reference: `colab.md`
- Models:
  - Attribute models: `apps/backend/models/attr_models/<ATTR>/`
  - OVR models: `apps/backend/models/ovr_models/{hit|pit}/{field_on|field_off}`
- DB models:
  - `PredictionRun`, `CardPrediction` in `apps/backend/src/database/models.py`

## Notes / Assumptions
- The newest `RosterUpdate` represents the last update; all MLB stats after that date are used for “since_” scopes.
- Each run is timestamped and stored; no upsert is performed.
- Model versioning is currently static (`model_version="ovr_v1"`) and can be made dynamic later.

## Minimal Model Files Required
For attribute models, only these are required at inference time:
- `final_model.joblib`
- `feature_cols.json`
- `best_model.json`

(Optionally keep `dataset_meta.json` for provenance.)
