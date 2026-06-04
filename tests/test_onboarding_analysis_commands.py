from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from pipeline.commands.onboarding_analysis import dispatch_onboarding_analysis_commands


class _ParserStub:
    def error(self, message: str) -> None:
        raise RuntimeError(message)


def _base_args() -> SimpleNamespace:
    return SimpleNamespace(
        enrich_game_from_wiki=None,
        wiki_url=None,
        wiki_manifest=None,
        wiki_source=None,
        adapt_game_schema=None,
        ingest_game_sources=None,
        source_manifest=None,
        curate_wiki_medal_draft=None,
        export_wiki_research_packet=None,
        curation_profile="multikill",
        bridge_wiki_draft_to_onboarding=None,
        build_onboarding_draft=None,
        report_unresolved_derived_rows=None,
        derive_game_detection_manifest=None,
        fill_derived_detection_rows=None,
        detection_id=None,
        fill_source_manifest=None,
        prepare_derived_row_review=None,
        summarize_derived_row_review=None,
        apply_derived_row_review=None,
        accept_recommended=False,
        only_auto_populated=False,
        reject_zero_candidate=False,
        defer_zero_candidate=False,
        onboard_game=None,
        publish_onboarding_draft=None,
        publish_onboarding_batch=None,
        validate_onboarding_publish=None,
        report_onboarding_batch=None,
        fuse_clip_signals=None,
        match_roi_templates=None,
        map_roi_events=None,
        analyze_roi_runtime=None,
        proxy_sidecar=None,
        runtime_sidecar=None,
        output_path=None,
        debug_output_dir=None,
        sample_fps=None,
        limit_frames=None,
        min_score=None,
        matcher_report=None,
        game=None,
        apply=False,
        full_json=False,
    )


def _dispatch(args: SimpleNamespace) -> int | None:
    return dispatch_onboarding_analysis_commands(
        args,
        parser=_ParserStub(),
        print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
        run_enrich_game_from_wiki_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_adapt_game_schema_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_ingest_game_sources_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_curate_wiki_medal_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_export_wiki_research_packet_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_bridge_wiki_draft_to_onboarding_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_build_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_unresolved_derived_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_derive_game_detection_manifest_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_fill_derived_detection_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_prepare_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_apply_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_onboard_game_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_publish_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_publish_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_validate_onboarding_publish_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_fuse_clip_signals_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_match_roi_templates_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_map_roi_events_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_analyze_roi_runtime_fn=lambda *a, **k: {"ok": True, "status": "ok"},
    )


class OnboardingAnalysisCommandTests(unittest.TestCase):
    def test_enrich_game_from_wiki_requires_source(self) -> None:
        args = _base_args()
        args.enrich_game_from_wiki = "marvel_rivals"
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args)
        self.assertEqual(str(ctx.exception), "--enrich-game-from-wiki requires --wiki-url, --wiki-manifest, or --wiki-source")

    def test_report_unresolved_derived_rows_uses_cli_renderer(self) -> None:
        args = _base_args()
        args.report_unresolved_derived_rows = "/tmp/draft"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["kwargs"]["command_name"], "report_unresolved_derived_rows")

    def test_bridge_wiki_draft_to_onboarding_uses_output_path(self) -> None:
        args = _base_args()
        args.bridge_wiki_draft_to_onboarding = "/tmp/wiki"
        args.output_path = "/tmp/onboarding"
        stdout = io.StringIO()
        calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        result_payload = {"ok": True, "status": "bindings_pending"}
        def _run_bridge(*a, **k):
            calls.append((a, k))
            return result_payload
        with redirect_stdout(stdout):
            exit_code = dispatch_onboarding_analysis_commands(
                args,
                parser=_ParserStub(),
                print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
                run_enrich_game_from_wiki_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_adapt_game_schema_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_ingest_game_sources_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_curate_wiki_medal_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_export_wiki_research_packet_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_bridge_wiki_draft_to_onboarding_fn=_run_bridge,
                run_build_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_report_unresolved_derived_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_derive_game_detection_manifest_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_fill_derived_detection_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_prepare_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_summarize_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_apply_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_onboard_game_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_publish_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_publish_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_validate_onboarding_publish_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_report_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_fuse_clip_signals_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_match_roi_templates_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_map_roi_events_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_analyze_roi_runtime_fn=lambda *a, **k: {"ok": True, "status": "ok"},
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(calls, [(("/tmp/wiki",), {"output_path": "/tmp/onboarding"})])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "bindings_pending")

    def test_curate_wiki_medal_draft_uses_profile_and_output_path(self) -> None:
        args = _base_args()
        args.curate_wiki_medal_draft = "/tmp/wiki"
        args.output_path = "/tmp/wiki_curated"
        args.curation_profile = "multikill"
        stdout = io.StringIO()
        calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        result_payload = {"ok": True, "status": "curated"}

        def _run_curate(*a, **k):
            calls.append((a, k))
            return result_payload

        with redirect_stdout(stdout):
            exit_code = dispatch_onboarding_analysis_commands(
                args,
                parser=_ParserStub(),
                print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
                run_enrich_game_from_wiki_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_adapt_game_schema_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_ingest_game_sources_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_curate_wiki_medal_draft_fn=_run_curate,
                run_export_wiki_research_packet_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_bridge_wiki_draft_to_onboarding_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_build_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_report_unresolved_derived_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_derive_game_detection_manifest_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_fill_derived_detection_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_prepare_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_summarize_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_apply_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_onboard_game_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_publish_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_publish_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_validate_onboarding_publish_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_report_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_fuse_clip_signals_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_match_roi_templates_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_map_roi_events_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_analyze_roi_runtime_fn=lambda *a, **k: {"ok": True, "status": "ok"},
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(calls, [(("/tmp/wiki",), {"output_path": "/tmp/wiki_curated", "profile": "multikill"})])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "curated")

    def test_export_wiki_research_packet_uses_output_path(self) -> None:
        args = _base_args()
        args.export_wiki_research_packet = "/tmp/wiki"
        args.output_path = "/tmp/research_packet"
        stdout = io.StringIO()
        calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        result_payload = {"ok": True, "status": "exported"}

        def _run_export(*a, **k):
            calls.append((a, k))
            return result_payload

        with redirect_stdout(stdout):
            exit_code = dispatch_onboarding_analysis_commands(
                args,
                parser=_ParserStub(),
                print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
                run_enrich_game_from_wiki_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_adapt_game_schema_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_ingest_game_sources_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_curate_wiki_medal_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_export_wiki_research_packet_fn=_run_export,
                run_bridge_wiki_draft_to_onboarding_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_build_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_report_unresolved_derived_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_derive_game_detection_manifest_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_fill_derived_detection_rows_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_prepare_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_summarize_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_apply_derived_row_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_onboard_game_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_publish_onboarding_draft_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_publish_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_validate_onboarding_publish_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_report_onboarding_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_fuse_clip_signals_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_match_roi_templates_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_map_roi_events_fn=lambda *a, **k: {"ok": True, "status": "ok"},
                run_analyze_roi_runtime_fn=lambda *a, **k: {"ok": True, "status": "ok"},
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(calls, [(("/tmp/wiki",), {"output_path": "/tmp/research_packet"})])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "exported")

    def test_fill_derived_detection_rows_requires_detection_ids(self) -> None:
        args = _base_args()
        args.fill_derived_detection_rows = "/tmp/draft"
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args)
        self.assertEqual(str(ctx.exception), "--fill-derived-detection-rows requires at least one --detection-id")

    def test_onboard_game_requires_source_manifest(self) -> None:
        args = _base_args()
        args.onboard_game = "marvel_rivals"
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args)
        self.assertEqual(str(ctx.exception), "--onboard-game requires --source-manifest")

    def test_analyze_roi_runtime_prints_json(self) -> None:
        args = _base_args()
        args.analyze_roi_runtime = ("/tmp/example.mp4", "marvel_rivals")
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ok")

    def test_returns_none_when_no_route_matches(self) -> None:
        self.assertIsNone(_dispatch(_base_args()))
