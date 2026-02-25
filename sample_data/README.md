# Sample data

This folder holds predefined sample datasets used by the pipeline and Web UI.

**Layout:**

- **child_birth/** — Child birth sample data (from DC repo `statvar_imports/child_birth/testdata/`):
  - `child_birth.tmcf`, `child_birth.csv`, `child_birth_stat_vars.mcf`
- **statistics_poland/** — Statistics Poland sample data (from DC repo `statvar_imports/statistics_poland/test/`):
  - `StatisticsPoland_output.tmcf`, `StatisticsPoland_output.csv`
  - `StatisticsPoland_output_stat_vars.mcf`, `StatisticsPoland_output_stat_vars_schema.mcf`
- **finland_census/** — Finland census sample data (from DC repo `statvar_imports/finland_census/test_data/`):
  - `finland_census_output.tmcf`, `finland_census_output.csv`
  - `finland_census_output_stat_vars.mcf`, `finland_census_output_stat_vars_schema.mcf`
- **uae_population/** — UAE population sample data (from DC repo `uae_bayanat/uae_population/test_data/`):
  - `uae_population_output.tmcf`, `uae_population_output.csv`
- **empty_differ.csv** — Used by the pipeline when no differ data.
- **README.md** — This file.

Run a predefined dataset:

```bash
./run_e2e_test.sh child_birth
./run_e2e_test.sh statistics_poland
./run_e2e_test.sh finland_census
./run_e2e_test.sh uae_population
```

Use **Custom** mode (Web UI or `./run_e2e_test.sh custom --tmcf=... --csv=...`) to validate your own TMCF + CSV files.
