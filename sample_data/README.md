# Sample data for child_birth rule-test datasets

This folder holds **child_birth**-based data (same structure as the DC data repo) with **one intentional error** per variant so a specific validation rule fails. Use them to test or demo the pipeline.

**Layout:**

- **child_birth/** — Clean child_birth testdata (TMCF, CSV, stat vars MCF). Used by the pipeline for the Child Birth and fail-min-value datasets so we don’t depend on the data repo for this sample data.
- **child_birth_ai_demo/** — TMCF + CSV with schema/typo issues for Gemini Review.
- **child_birth_fail_min_value/** — CSV only (TMCF from `child_birth/`); one negative value + large fluctuations.
- **child_birth_fail_units/** — TMCF + CSV with mixed units.
- **child_birth_fail_scaling_factor/** — TMCF + CSV with inconsistent scaling factor.
- **mcf/** — Optional stat vars MCFs for **Custom** mode (schema/name/description tests, percent denominator).
- **empty_differ.csv** — Used by the pipeline when no differ data.
- **README.md** — This file.

The **Child Birth** dataset (the main “clean” one) uses only TMCF + CSV from this repo's **child_birth/** and **no stat vars MCF**, so it runs as one clean success with no AI Review stat_var warnings. The fail/AI-demo variants use **child_birth/child_birth_stat_vars.mcf** from this repo when the script supplies it.

---

## What’s wrong in each variant

| Path | What’s wrong | Rule that fails |
|------|----------------|------------------|
| **child_birth_fail_min_value/child_birth_fail_min_value.csv** | One row **value = -1** (USA 2023-01 `Count_BirthEvent_LiveBirth`). Two rows with **large fluctuations** (e.g. USA 2023-03 `Count_Death` 300%; USA 2023-04 `Count_Death_Upto1Years` 200%). | **check_min_value** |
| **child_birth_fail_units/** (.tmcf + .csv) | One row has **unit = "Percent"** for a count; rest empty. Units must be consistent per StatVar. | **check_unit_consistency** |
| **child_birth_fail_scaling_factor/** (.tmcf + .csv) | One month has **scalingFactor = 100**, others **1** for same StatVar. | **check_scaling_factor_consistency** |
| **child_birth_ai_demo/** (.tmcf + .csv) | TMCF: missing **dcs:**, duplicate **observationDate**, typo **scaleingFactor**. | **Gemini Review** (schema & typos) |
| **mcf/sample_stat_vars.mcf** | Intentional gaps: name only, name+description, or all three (name/description/alternateName). | Stat vars MCF validation (advisory); missing_statvar_definition |
| **mcf/sample_stat_vars_with_percent.mcf** | Percent StatVar with **no measurementDenominator**. | **missing_measurement_denominator** |
| **mcf/sample_stat_vars_schema.mcf** | Schema MCF with intentional gaps (name only, etc.). | Stat vars schema MCF validation (advisory) |

The **child_birth_fail_min_value** variant uses **child_birth/child_birth.tmcf** and **child_birth/child_birth_stat_vars.mcf** from this repo; the units, scaling_factor, and AI demo variants use the TMCF/CSV in their own subfolders (same schema, with the intentional error for each case).

See the main [README](../README.md) → “What bad data is in each child_birth variant” for more detail.

---

## How to test MCF validation (Custom mode, UI)

**Required:** TMCF + CSV (e.g. `child_birth_ai_demo/child_birth_ai_demo.tmcf` + `child_birth_ai_demo/child_birth_ai_demo.csv`). Turn **Gemini Review** ON, then run.

| Upload | What runs | What to check in Report → AI Review (Gemini) |
|--------|-----------|-----------------------------------------------|
| **2 files:** TMCF + CSV only | TMCF/CSV + Gemini on TMCF | Only TMCF issues (e.g. typos, namespace). No MCF validation. |
| **3 files:** TMCF + CSV + **Stat vars MCF** | Same + stat_vars MCF (name/description/alternateName; generated vs defined StatVars; percent/rate measurementDenominator) | Issues for `input_stat_vars.mcf` (e.g. missing description; missing_statvar_definition; missing_measurement_denominator). |
| **3 files:** TMCF + CSV + **Stat vars schema MCF** | Same + schema MCF name/description/alternateName | Issues for `input_stat_vars_schema.mcf`. |
| **4 files:** TMCF + CSV + Stat vars MCF + Stat vars schema MCF | Both MCF validations | Issues for both MCFs. |

Use **mcf/sample_stat_vars.mcf**, **mcf/sample_stat_vars_with_percent.mcf**, and **mcf/sample_stat_vars_schema.mcf** as the optional MCFs to get predictable advisory warnings.
