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
- Step 1 is `chore(init)`; follow the 10-step blueprint protocol for the rest.

## Environment & storage hygiene

- All caches, virtualenvs and temp files live in `./.cache/` (pinned by `.envrc`):
  venv `./.cache/venv`, uv `./.cache/uv`, Python `./.cache/python`, plus ruff, mypy,
  pytest, coverage, pycache, XDG dirs and `TMPDIR`. Never create `.venv/` or write
  caches elsewhere.
- Docker state stays on the SSD: `COLIMA_HOME=/Volumes/KINGSTONE_SSD/.colima`,
  buildx config in `./.cache/buildx`.
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
