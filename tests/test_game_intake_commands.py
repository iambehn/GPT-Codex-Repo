from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from pipeline.commands.game_intake import dispatch_game_intake_commands


def _base_args() -> SimpleNamespace:
    return SimpleNamespace(
        list_games=False,
        init_game=None,
        validate_game_pack=None,
        scan_chat_log=None,
        scan_vod=None,
        scan_vod_batch=None,
        export_training_data=None,
        full_json=False,
        chat_log=None,
        pattern=None,
        limit=None,
        game=None,
    )


def _dispatch(args: SimpleNamespace) -> int | None:
    return dispatch_game_intake_commands(
        args,
        print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
        list_games_fn=lambda: ["marvel_rivals"],
        init_game_pack_fn=lambda game: {"ok": True, "game": game},
        validate_game_pack_fn=lambda game: {"ok": True, "game": game},
        run_scan_chat_log_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_scan_vod_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_scan_vod_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_export_training_data_fn=lambda *a, **k: {"ok": True, "status": "ok"},
    )


class GameIntakeCommandTests(unittest.TestCase):
    def test_list_games_prints_json_payload(self) -> None:
        args = _base_args()
        args.list_games = True
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload, {"ok": True, "games": ["marvel_rivals"]})

    def test_scan_vod_batch_routes_through_cli_renderer(self) -> None:
        args = _base_args()
        args.scan_vod_batch = ("/tmp/clips", "marvel_rivals")
        args.limit = 5
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["kwargs"]["command_name"], "scan_vod_batch")
        self.assertTrue(payload["result"]["ok"])

    def test_export_training_data_prints_json_payload(self) -> None:
        args = _base_args()
        args.export_training_data = "/tmp/sidecars"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ok")

    def test_returns_none_when_no_route_matches(self) -> None:
        self.assertIsNone(_dispatch(_base_args()))
