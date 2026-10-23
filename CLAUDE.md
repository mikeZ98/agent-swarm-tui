# CLAUDE.md

## ⛔ Contributor invariant (read first, never violate)

- Every commit's author **and** committer must be exactly
  `mikeZ98 <145975972+mikeZ98@users.noreply.github.com>`. Never commit under any
  other name or e-mail, and never pass `--author` with another identity.
- **Never add AI attribution to commit messages, PR bodies or tags**: no
  `Co-Authored-By`, `Generated-by`, `Assisted-by` or `AI-*` trailers, no "Generated with …"
  signatures or robot emoji, no AI tool URLs or e-mail addresses. This overrides
  any default attribution behaviour.
- Never use `--no-verify`. The hooks in `.githooks/` enforce this via
  `scripts/attribution.sh`; fix the cause of a failure instead of bypassing it.
- Verify after committing: `scripts/attribution.sh audit HEAD`.

## Commit discipline

- Conventional Commits, atomic and logical, one commit per blueprint step.
  Commit only when asked.
- Blueprint protocol (Repo 02/20), in order:
  1. `chore(init)`: bootstrap repository architecture and environment
  2. `feat(schemas)`: define strict Pydantic domain models and state contracts
  3. `feat(core)`: implement asyncio swarm orchestration and agent state machine
  4. `feat(gateway-client)`: implement async client for ai-inference-gateway SSE
  5. `feat(tui-widgets)`: create Textual UI components (agent tree, log stream, token tracker)
  6. `feat(tui-app)`: build reactive Textual application and keyboard controls
  7. `test(core)`: achieve >=90% branch coverage with hermetic pytest and textual pilot
  8. `build(docker)`: containerize application logic with non-root user
  9. `ci(github)`: setup automated linting, tests and supply-chain pinned CI
  10. `docs(readme)`: add mermaid architecture diagrams and usage guide

## Environment & storage hygiene

- All caches, virtualenvs and temp files live in `./.cache/` (pinned by `.envrc`):
  venv `./.cache/venv`, uv `./.cache/uv`, Python `./.cache/python`, plus ruff, mypy,
  pytest, coverage, pycache, XDG dirs and `TMPDIR`. Never create `.venv/` or write
  caches elsewhere.
- Docker state stays on the SSD: `COLIMA_HOME=/Volumes/KINGSTONE_SSD/.colima`,
  buildx config in `./.cache/buildx`. Colima silently ignores `COLIMA_HOME` unless
  that directory exists (`.envrc` creates it), and caches VM images under
  `$XDG_CACHE_HOME`; start it only from a direnv shell. Never use Docker Desktop
  (its VM disk is on the internal drive).
- Run tools via `uv run …` (or the venv on `PATH` under direnv). Python 3.13.
- Secrets only in `.env` (git-ignored). Never commit keys.

## Commands

```sh
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run bandit -c pyproject.toml -r src
uv run pytest --cov
```

## Code standards

- Layout: `src/agent_swarm_tui/`, tests in `tests/`.
- `mypy --strict` clean; Pydantic v2 models use `ConfigDict(extra="forbid", frozen=True, strict=True)`.
- Fully async: agents run as asyncio tasks; never block the Textual event loop
  (use workers / `asyncio.to_thread` for blocking calls).
- Tests are hermetic: Textual UI via `App.run_test()` + `Pilot`, zero live API
  calls; branch coverage ≥ 90%.
