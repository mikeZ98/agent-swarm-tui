"""CLI parsing, app construction, logging setup and entrypoint exit codes."""

import asyncio
import logging
import runpy
import sys
from pathlib import Path

import pytest
from textual.app import App
from textual.logging import TextualHandler

from agent_swarm_tui import SwarmApp, __version__, main
from agent_swarm_tui.app import EXIT_USAGE, _configure_logging, build_app, build_parser
from agent_swarm_tui.schemas import RoutingStrategy
from tests.conftest import wait_for


class TestParser:
    def test_defaults(self) -> None:
        args = build_parser().parse_args([])
        assert (args.topology, args.name, args.task, args.gateway_url) == (
            "sequential",
            "swarm",
            None,
            None,
        )
        assert (args.cost_per_million, args.log_file, args.log_level) == (0.0, None, "WARNING")

    def test_all_options(self) -> None:
        args = build_parser().parse_args(
            [
                "--gateway-url", "http://127.0.0.1:9",
                "--topology", "hierarchical",
                "--name", "crew",
                "--task", "do it",
                "--model", "m",
                "--temperature", "0.5",
                "--max-tokens", "64",
                "--cost-per-million", "3",
                "--drain-timeout", "2",
            ]
        )  # fmt: skip
        assert (args.topology, args.temperature, args.max_tokens, args.drain_timeout) == (
            "hierarchical",
            0.5,
            64,
            2.0,
        )

    @pytest.mark.parametrize(
        "argv", [["--topology", "star"], ["--temperature", "hot"], ["--bogus"]]
    )
    def test_argparse_errors_exit_2(
        self, argv: list[str], capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as caught:
            build_parser().parse_args(argv)
        assert caught.value.code == 2
        assert "usage: agent-swarm-tui" in capsys.readouterr().err

    def test_version(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as caught:
            main(["--version"])
        assert caught.value.code == 0
        assert capsys.readouterr().out.strip() == f"agent-swarm-tui {__version__}"


class TestBuildApp:
    async def test_builds_from_arguments(self) -> None:
        args = build_parser().parse_args(
            [
                "--gateway-url",
                "http://127.0.0.1:9",
                "--topology",
                "mesh",
                "--name",
                "crew",
                "--model",
                "m-1",
            ]
        )
        app = build_app(args)
        assert isinstance(app, SwarmApp)
        assert app.swarm.topology.routing_strategy is RoutingStrategy.MESH
        assert app.swarm.topology.name == "crew"
        assert app._client is not None
        assert str(app._client.settings.base_url) == "http://127.0.0.1:9/"
        await app._close_client()

    def test_environment_gateway_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GATEWAY_BASE_URL", "http://127.0.0.1:7777")
        app = build_app(build_parser().parse_args([]))
        assert app._client is not None
        assert str(app._client.settings.base_url) == "http://127.0.0.1:7777/"
        asyncio.run(app._close_client())


class TestMain:
    @pytest.mark.parametrize(
        ("argv", "message"),
        [
            (["--gateway-url", "not a url"], "valid URL"),
            (["--gateway-url", "http://u:p@host:1"], "must not embed credentials"),
            (["--temperature", "9"], "less than or equal to 2"),
            (["--max-tokens", "0"], "greater than or equal to 1"),
            (["--task", "   "], "--task must not be blank"),
            (["--cost-per-million", "-1"], "cost_per_million_tokens"),
        ],
    )
    def test_invalid_configuration_exits_2(
        self, argv: list[str], message: str, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert main(argv) == EXIT_USAGE
        err = capsys.readouterr().err
        assert err.startswith("agent-swarm-tui: invalid configuration:")
        assert message in err

    def test_runs_the_app_and_returns_its_exit_code(
        self, monkeypatch: pytest.MonkeyPatch, restore_logging: None, tmp_path: Path
    ) -> None:
        seen: list[SwarmApp] = []

        def headless_run(self: SwarmApp) -> None:
            async def drive() -> None:
                async with self.run_test() as pilot:
                    await wait_for(lambda: self.swarm_status == "running")
                    await pilot.press("q")
                    await wait_for(lambda: self.return_code is not None)

            seen.append(self)
            asyncio.run(drive())

        monkeypatch.setattr(SwarmApp, "run", headless_run)
        log_file = tmp_path / "swarm.log"
        assert main(["--name", "cli", "--log-file", str(log_file), "--log-level", "INFO"]) == 0
        assert seen[0].swarm.topology.name == "cli"
        assert seen[0]._client_closed
        assert log_file.exists()

    def test_return_code_none_maps_to_zero(
        self, monkeypatch: pytest.MonkeyPatch, restore_logging: None
    ) -> None:
        def no_run(self: SwarmApp) -> None:
            asyncio.run(self._close_client())

        monkeypatch.setattr(SwarmApp, "run", no_run)
        assert main([]) == 0


class TestLogging:
    def test_file_handler(self, tmp_path: Path, restore_logging: None) -> None:
        log_file = tmp_path / "out.log"
        _configure_logging(str(log_file), "INFO")
        logging.getLogger("agent_swarm_tui.test").info("hello file")
        for handler in logging.getLogger().handlers:
            handler.flush()
        assert "INFO agent_swarm_tui.test: hello file" in log_file.read_text()

    async def test_default_routes_through_the_running_app(
        self, restore_logging: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _configure_logging(None, "WARNING")
        assert any(isinstance(h, TextualHandler) for h in logging.getLogger().handlers)
        async with App[None]().run_test():
            logging.getLogger("agent_swarm_tui.test").warning("while running")
        assert "while running" not in capsys.readouterr().err  # never onto the terminal UI

    def test_package_logger_has_a_null_handler(self) -> None:
        assert any(
            isinstance(h, logging.NullHandler)
            for h in logging.getLogger("agent_swarm_tui").handlers
        )


def test_python_dash_m(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["agent-swarm-tui", "--version"])
    with pytest.raises(SystemExit) as caught:
        runpy.run_module("agent_swarm_tui", run_name="__main__")
    assert caught.value.code == 0
    assert __version__ in capsys.readouterr().out
