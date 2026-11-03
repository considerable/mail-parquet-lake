import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "dashboard_builder", Path(__file__).parents[1] / "scripts/build_dashboard.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


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
