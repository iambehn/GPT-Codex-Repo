from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

from pipeline.commands.maintenance import dispatch_maintenance_commands, run_audit_pipeline_contracts


class MaintenanceCommandTests(unittest.TestCase):
    def test_run_audit_pipeline_contracts_writes_requested_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            output_path = root / "reports" / "audit.json"
            debug_dir = root / "debug"

            result = run_audit_pipeline_contracts(
                game="marvel_rivals",
                output_path=output_path,
                debug_output_dir=debug_dir,
                repo_root=root,
                config_payload={"proxy_scanner": {"signals": {"audio_prepass": {"enabled": True}}}},
                auditor=lambda **_: {"ok": True, "status": "ok", "game_filter": "marvel_rivals"},
            )

            self.assertTrue(result["ok"])
            self.assertTrue(output_path.is_file())
            self.assertTrue((debug_dir / "pipeline_contract_audit.json").is_file())
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8"))["status"], "ok")

    def test_dispatch_maintenance_commands_routes_rendered_output_commands(self) -> None:
        args = SimpleNamespace(
            audit_pipeline_contracts=False,
            inspect_quality_maintenance_findings=True,
            run_decision_regression_goldsets=False,
            run_repo_quality_health=False,
            check_roi_runtime=False,
            validate_published_pack=None,
            list_pack_templates=None,
            game="marvel_rivals",
            json=False,
            output_path=None,
            debug_output_dir=None,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = dispatch_maintenance_commands(
                args,
                run_audit_pipeline_contracts_fn=lambda **_: {"ok": True},
                run_inspect_quality_maintenance_findings_fn=lambda **_: {"ok": True, "rendered_output": "maintenance ok"},
                run_decision_regression_goldsets_fn=lambda **_: {"ok": True, "rendered_output": "decision ok"},
                run_repo_quality_health_fn=lambda **_: {"ok": True, "rendered_output": "repo ok"},
                run_check_roi_runtime_fn=lambda: {"ok": True},
                run_validate_published_pack_fn=lambda game: {"ok": True, "game": game},
                run_list_pack_templates_fn=lambda game: {"ok": True, "game": game},
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue().strip(), "maintenance ok")

    def test_dispatch_maintenance_commands_returns_none_when_no_route_matches(self) -> None:
        args = SimpleNamespace(
            audit_pipeline_contracts=False,
            inspect_quality_maintenance_findings=False,
            run_decision_regression_goldsets=False,
            run_repo_quality_health=False,
            check_roi_runtime=False,
            validate_published_pack=None,
            list_pack_templates=None,
            game=None,
            json=False,
            output_path=None,
            debug_output_dir=None,
        )
        exit_code = dispatch_maintenance_commands(
            args,
            run_audit_pipeline_contracts_fn=lambda **_: {"ok": True},
            run_inspect_quality_maintenance_findings_fn=lambda **_: {"ok": True, "rendered_output": "maintenance ok"},
            run_decision_regression_goldsets_fn=lambda **_: {"ok": True, "rendered_output": "decision ok"},
            run_repo_quality_health_fn=lambda **_: {"ok": True, "rendered_output": "repo ok"},
            run_check_roi_runtime_fn=lambda: {"ok": True},
            run_validate_published_pack_fn=lambda game: {"ok": True, "game": game},
            run_list_pack_templates_fn=lambda game: {"ok": True, "game": game},
        )
        self.assertIsNone(exit_code)
