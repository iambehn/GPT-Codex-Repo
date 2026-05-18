from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable


def dispatch_game_intake_commands(
    args: argparse.Namespace,
    *,
    print_cli_result_fn: Callable[..., None],
    list_games_fn: Callable[[], list[str]],
    init_game_pack_fn: Callable[[str], dict[str, Any]],
    validate_game_pack_fn: Callable[[str], dict[str, Any]],
    run_scan_chat_log_fn: Callable[..., dict[str, Any]],
    run_scan_vod_fn: Callable[..., dict[str, Any]],
    run_scan_vod_batch_fn: Callable[..., dict[str, Any]],
    run_export_training_data_fn: Callable[..., dict[str, Any]],
) -> int | None:
    if args.list_games:
        print(json.dumps({"ok": True, "games": list_games_fn()}, indent=2))
        return 0

    if args.init_game:
        print(json.dumps(init_game_pack_fn(args.init_game), indent=2))
        return 0

    if args.validate_game_pack:
        print(json.dumps(validate_game_pack_fn(args.validate_game_pack), indent=2))
        return 0

    if args.scan_chat_log:
        log_path, game = args.scan_chat_log
        print_cli_result_fn(
            run_scan_chat_log_fn(Path(log_path), game),
            command_name="scan_chat_log",
            full_json=args.full_json,
        )
        return 0

    if args.scan_vod:
        source, game = args.scan_vod
        print_cli_result_fn(
            run_scan_vod_fn(source, game, chat_log=args.chat_log),
            command_name="scan_vod",
            full_json=args.full_json,
        )
        return 0

    if args.scan_vod_batch:
        root, game = args.scan_vod_batch
        print_cli_result_fn(
            run_scan_vod_batch_fn(root, game, pattern=args.pattern, limit=args.limit),
            command_name="scan_vod_batch",
            full_json=args.full_json,
        )
        return 0

    if args.export_training_data:
        print(json.dumps(run_export_training_data_fn(args.export_training_data, game=args.game), indent=2))
        return 0

    return None
