# AGENTS.md

Guidance for AI agents working in this repository.

## Project

- Distribution: `reliable-ai-streams-py` (PyPI). Import package: **`l0`**, source in `src/l0/`.
- Reliability layer for LLM streaming: retry/fallback, guardrails, structured output, consensus, event sourcing, observability.
- Python `>=3.10`, typed (`py.typed`), built with hatchling, dependencies managed by **uv** (`uv.lock` is committed).

## Commands

```bash
uv sync --all-extras                 # full dev environment (openai, litellm, otel, sentry, speed)
uv run pytest --ignore=tests/integration   # what CI runs: 1853 passed, 7 skipped, ~95s
uv run pytest tests/test_guardrails.py -q  # single file
uv run pytest --cov=l0 --cov-report=term-missing
uv run mypy src/l0                   # strict; advisory, 20 pre-existing errors (see below)
uv run ruff check <files you changed> # advisory; scope it to your diff, never the whole tree
```

Use `uv run --no-sync ...` to skip re-resolution when the venv is already populated.

## Repository state — read before "fixing" anything

CI (`.github/workflows/test.yml`) runs **pytest only**, on Python 3.10–3.13 plus an `openai` 2.x/3.x matrix. There is no lint or type-check job.

The tree is not lint- or type-clean today:

- `ruff check src/l0 tests` → 407 pre-existing errors (mostly `E501`, `UP037`, `F401`).
- `mypy src/l0` (strict) → 20 pre-existing errors in 8 files.

Therefore:

- **Never bulk-fix these findings**: no `ruff check --fix` across `src/l0`/`tests`, and no `ruff format` on files you did not change (5 files are formatter-dirty today — reformatting them is unrelated churn in your PR).
- Keep *your own* diff clean: run `ruff check`/`mypy` scoped to the files you changed and address only findings on lines you wrote; match surrounding style by hand (88-col, ruff `E,F,I,UP,B,SIM`).
- A failing `pytest` run is a real regression; a nonzero `ruff`/`mypy` exit is not, by itself.

## Testing

- `pytest` config lives in `pyproject.toml`: `asyncio_mode = "auto"` (async tests need **no** `@pytest.mark.asyncio`), `testpaths = ["tests"]`.
- Unit tests: `tests/test_*.py`. Shared fixtures and capability probes in `tests/conftest.py` (`has_openai`, `has_litellm`, `requires_openai`, …); JSON fixtures in `tests/fixtures/`.
- `tests/integration/` hits real provider APIs, requires `OPENAI_API_KEY` (loaded from `.env` via python-dotenv) and is excluded from CI. Do not add tests there that must pass offline.
- Stray `test_structured.py` and `test_network_errors.py` at the repo root are ad-hoc scripts, **not** collected by pytest (`testpaths` is `tests`). Don't add new ones; put tests under `tests/`.

## Architecture

```
src/l0/
  __init__.py       # lazy public API: _API_MODULES map, __getattr__, TYPE_CHECKING imports, __all__
                    # also defines the top-level entry points wrap() and run()
  api/              # thin facade modules; re-export implementation symbols with explicit __all__
  runtime.py        # core run loop (_internal_run), lifecycle callbacks, timeouts
  retry.py errors.py guardrails.py _structured.py consensus.py continuation.py
  parallel.py pipeline.py pool.py window.py multimodal.py drift.py comparison.py
  adapters.py client.py stream.py types.py normalize.py _utils.py version.py
  state.py state_machine.py events.py metrics.py logging.py json_schema.py
  format.py          # namespace object re-exporting formatting/
  event_sourcing.py  # backs the l0.* event-sourcing exports via api/event_sourcing.py
  formatting/        # format helpers (context, memory, output, strings, tools)
  monitoring/        # otel, sentry, exporter, dispatcher, telemetry
  eventsourcing/     # SEPARATE deterministic replay package, reached only as `l0.eventsourcing.*`
  pydantic/          # BaseModel mirrors of the dataclass types (validation/schema)
```

Note: `CONTRIBUTING.md` still lists `structured.py`; the actual module is `_structured.py`, exposed through `api/structured.py`.

### Exporting a public symbol (three places, all required)

`l0/__init__.py` imports lazily, so a new public name must be added to **all three**:

1. `_API_MODULES` — `"SymbolName": "api.<module>"` (drives `__getattr__`).
2. The `if _TYPE_CHECKING:` import block — so type checkers and IDEs see it.
3. `__all__` at the bottom of the file.

Plus the symbol must be re-exported from the matching `src/l0/api/<module>.py` facade (import + `__all__`). Missing step 1 → `AttributeError` at runtime; missing step 2 → type-checker errors for users; missing step 3 → absent from `from l0 import *` and docs tooling.

Implementation modules must not import `l0/__init__.py`; the facade layer exists to keep import cycles out.

## Conventions

- Google-style docstrings with `Args` / `Returns` / `Raises` on all public functions.
- Explicit type hints everywhere; `|` unions (3.10+), `collections.abc` for abstract types, avoid `Any`.
- `snake_case` functions/modules, `PascalCase` classes and type aliases, `UPPER_SNAKE_CASE` constants, `_prefix` for private.
- ruff: `target-version = py310`, `line-length = 88`, rules `E,F,I,UP,B,SIM`.
- Import order: stdlib, third-party, local.
- Errors: raise the specific classes from `l0/errors.py` with contextual messages.

## Scope policy (from CONTRIBUTING.md)

Core stays small and integration-agnostic. **Do not add** database adapters, cloud-service integrations, monitoring backends, or provider-specific extensions to this repo — those live out-of-tree. In-scope: runtime features, guardrail rules, format helpers, type definitions, core utilities.

## Docs

Documentation is a set of top-level Markdown files, not a docs site. When behavior changes, update the relevant one: `API.md` (full reference), `README.md`, `QUICKSTART.md`, and the topic files (`GUARDRAILS.md`, `STRUCTURED_OUTPUT.md`, `ERROR_HANDLING.md`, `MONITORING.md`, `EVENT_SOURCING.md`, `CONSENSUS.md`, `MULTIMODAL.md`, `DOCUMENT_WINDOWS.md`, …).

## Versioning

Two places hold the version: `pyproject.toml` `[project].version` and `src/l0/version.py` (`__version__`). Bump them together. The release workflow overwrites the `pyproject.toml` value from the release tag before building, so `version.py` is the one that must be correct in the commit.
