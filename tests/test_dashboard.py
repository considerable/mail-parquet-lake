import unittest
from unittest.mock import patch

from scripts import build_dashboard as module


class DashboardTests(unittest.TestCase):
    def test_public_allowlist_excludes_sensitive_extra_fields(self):
        run = {
            "id": 1,
            "name": "Sync",
            "actor": {"email": "private"},
            "inputs": {"token": "secret"},
            "logs_url": "secret",
        }
        result = module.public_run(run)
        self.assertEqual(result["id"], 1)
        self.assertNotIn("actor", result)
        self.assertNotIn("inputs", result)
        self.assertNotIn("logs_url", result)
        job = module.public_job(
            {
                "steps": [
                    {"name": "Run sync", "conclusion": "failure", "output": "private"}
                ]
            }
        )
        self.assertNotIn("output", job["steps"][0])

    def test_truncated_history_and_correct_attempt_endpoint(self):
        run = {"id": 8, "run_attempt": 2}
        first_page = {"total_count": 101, "workflow_runs": [run] * 100}
        with patch.object(
            module,
            "api",
            side_effect=[
                first_page,
                {"total_count": 1, "jobs": []},
                {"total_count": 0, "workflows": []},
            ],
        ) as mock:
            result = module.collect("owner/repo", max_pages=1, detail_limit=1)
        self.assertTrue(result["history_truncated"])
        self.assertFalse(result["runs"][0]["job_details_complete"])
        self.assertIn("/attempts/2/jobs", mock.call_args_list[1].args[0])

    def test_optional_detail_failure_preserves_history(self):
        with patch.object(
            module,
            "api",
            side_effect=[
                {"total_count": 1, "workflow_runs": [{"id": 1}]},
                OSError("secret"),
                {"total_count": 0, "workflows": []},
            ],
        ):
            result = module.collect("owner/repo")
        self.assertEqual(len(result["runs"]), 1)
        self.assertEqual(len(result["warnings"]), 1)
        self.assertNotIn("secret", str(result))

    def test_disabled_state_survives_successful_run(self):
        with patch.object(
            module,
            "api",
            side_effect=[
                {
                    "total_count": 1,
                    "workflow_runs": [{"id": 1, "conclusion": "success"}],
                },
                {
                    "total_count": 1,
                    "workflows": [
                        {
                            "id": 2,
                            "name": "Gmail Sync",
                            "state": "disabled_inactivity",
                            "private": "secret",
                        }
                    ],
                },
            ],
        ):
            result = module.collect("owner/repo", detail_limit=0)
        self.assertTrue(result["workflow_states_available"])
        self.assertEqual(result["workflows"][0]["state"], "disabled_inactivity")
        self.assertNotIn("private", result["workflows"][0])

    def test_unavailable_workflow_state_is_unknown(self):
        with patch.object(
            module,
            "api",
            side_effect=[{"total_count": 0, "workflow_runs": []}, OSError("secret")],
        ):
            result = module.collect("owner/repo")
        self.assertFalse(result["workflow_states_available"])
        self.assertEqual(result["workflows"], [])
        self.assertNotIn("secret", str(result))


if __name__ == "__main__":
    unittest.main()


class ApiSecurityTests(unittest.TestCase):
    def test_api_uses_fixed_https_host_and_disables_redirects(self):
        from unittest.mock import MagicMock

        response = MagicMock(status_code=200)
        response.json.return_value = {"workflow_runs": []}
        with (
            patch.dict("os.environ", {"GH_TOKEN": "test-token"}),
            patch.object(module.requests, "get", return_value=response) as get,
        ):
            self.assertEqual(
                module.api("repos/owner/repo/actions/runs"), {"workflow_runs": []}
            )
        self.assertEqual(
            get.call_args.args[0],
            "https://api.github.com/repos/owner/repo/actions/runs",
        )
        self.assertFalse(get.call_args.kwargs["allow_redirects"])
        self.assertEqual(get.call_args.kwargs["timeout"], 30)

    def test_redirect_does_not_forward_credentials(self):
        from unittest.mock import MagicMock

        with (
            patch.dict("os.environ", {"GH_TOKEN": "test-token"}),
            patch.object(
                module.requests, "get", return_value=MagicMock(status_code=302)
            ),
            self.assertRaises(OSError),
        ):
            module.api("repos/owner/repo/actions/runs")


class ExecutiveArtifactTests(unittest.TestCase):
    def test_untrusted_branch_artifact_is_not_downloaded(self):
        with (
            patch.object(
                module,
                "api",
                return_value={
                    "workflow_runs": [
                        {
                            "id": 1,
                            "path": ".github/workflows/executive.yml",
                            "head_branch": "untrusted",
                            "head_repository": {"full_name": "owner/repo"},
                        }
                    ]
                },
            ),
            patch.object(module.subprocess, "run") as run,
        ):
            with self.assertRaises(ValueError):
                module.collect_executive("owner/repo")
            run.assert_not_called()

    def test_trusted_artifact_is_reprojected_before_publication(self):
        import json
        from datetime import UTC, datetime
        from pathlib import Path

        source = {
            "schema_version": 1,
            "observed_at": datetime.now(UTC).isoformat(),
            "evidence_available": False,
            "email_subject": "PRIVATE-SUBJECT",
            "health": {"reasons": ["PRIVATE-REASON"]},
            "history": [],
        }

        def download(args, **kwargs):
            directory = Path(args[args.index("--dir") + 1])
            (directory / "executive-public.json").write_text(json.dumps(source))

        with (
            patch.object(
                module,
                "api",
                side_effect=[
                    {
                        "workflow_runs": [
                            {
                                "id": 1,
                                "path": ".github/workflows/executive.yml",
                                "head_branch": "main",
                                "head_repository": {"full_name": "owner/repo"},
                            }
                        ]
                    },
                    {
                        "artifacts": [
                            {"name": "mail-lake-executive-public", "expired": False}
                        ]
                    },
                ],
            ),
            patch.object(module.subprocess, "run", side_effect=download),
        ):
            result = module.collect_executive("owner/repo")
        self.assertNotIn("PRIVATE-", json.dumps(result))
        self.assertEqual(result["health"]["status"], "UNKNOWN")
