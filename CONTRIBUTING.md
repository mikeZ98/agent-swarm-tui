# Contributing

> [!IMPORTANT]
> **Sole-contributor identity: no exceptions.**
>
> Every commit's **author and committer** must be exactly:
>
> | Field | Value                                         |
> | ----- | --------------------------------------------- |
> | Name  | `mikeZ98`                                     |
> | Email | `145975972+mikeZ98@users.noreply.github.com`  |
>
> No other names, machine usernames or local/personal e-mail addresses.
> Commit messages follow Conventional Commits. AI-native development is
> welcome and disclosed: tool-assistance trailers such as `Co-Authored-By:`
> may stay in the message, and every change passes the same automated gates.
>
> Every enforcement point runs the same `scripts/attribution.sh`:
>
> 1. `.githooks/pre-commit` checks the pending author and committer identity.
> 2. `.githooks/commit-msg` checks identity and the Conventional Commits format.
> 3. `.githooks/pre-push` audits every outgoing commit, including ones made with `--no-verify`.
> 4. The CI `attribution-audit` job re-checks every pushed or pull-request commit.

## Setup

Requirements: [`uv`](https://docs.astral.sh/uv/) and [`direnv`](https://direnv.net/).

```sh
direnv allow   # pins caches to ./.cache/, installs hooks, sets repo-local identity
uv sync        # creates ./.cache/venv with runtime + dev dependencies
```

`.envrc` puts every cache, virtualenv, Python install and temp directory
under `./.cache/` (uv, ruff, mypy, pytest, coverage, pycache, XDG, `TMPDIR`), and
Docker/Colima state under `$DOCKER_STATE_ROOT/.colima` (default `$HOME`; set it to an
external volume in the git-ignored `.envrc.local`). It also runs:

```sh
git config --local core.hooksPath .githooks
git config --local user.name  mikeZ98
git config --local user.email 145975972+mikeZ98@users.noreply.github.com
```

If you don't use direnv, run those three commands by hand. The test suite is
hermetic: it blocks real network access and needs no running gateway.

Secrets go in `.env` (git-ignored, auto-loaded by direnv).

## Quality gates

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy --strict src/ tests/
uv run bandit -c pyproject.toml -r src/
uv run pytest --cov=agent_swarm_tui --cov-branch --cov-report=term-missing --cov-fail-under=90
uv tool run --from hadolint-bin==2.15.1 hadolint --failure-threshold info Dockerfile
```

## Commits

[Conventional Commits](https://www.conventionalcommits.org/), atomic and
logical: `<type>(<scope>)?: <description>` where type is one of
`feat fix docs style refactor perf test build ci chore revert`.

To audit history manually:

```sh
scripts/attribution.sh audit HEAD          # whole branch
scripts/attribution.sh audit origin/main..HEAD
```

If a commit violates the invariant, rewrite it (`git commit --amend
--reset-author`, or an interactive rebase) before pushing.
