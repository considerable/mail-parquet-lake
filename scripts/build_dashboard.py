"""Build a public-safe Actions history snapshot; never collect logs or artifacts."""

import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError

import requests

try:
    from .public_executive import sanitize_public
except ImportError:
    from public_executive import sanitize_public


def api(path):
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        return json.loads(subprocess.check_output(["gh", "api", path], text=True))
    # The host is fixed; paths cannot select a URL scheme or redirect credentials.
    response = requests.get(
        "https://api.github.com/" + path,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30,
        allow_redirects=False,
    )
    if 300 <= response.status_code < 400:
        raise URLError("Unexpected GitHub API redirect")
    response.raise_for_status()
    return response.json()


def public_run(run):
    # Explicit allowlist: no actor profiles, commit messages, input data, or logs.
    keys = (
        "id",
        "name",
        "path",
        "event",
        "status",
        "conclusion",
        "run_number",
        "run_attempt",
        "created_at",
        "run_started_at",
        "updated_at",
        "html_url",
    )
    return {key: run.get(key) for key in keys}


def public_job(job):
    keys = (
        "id",
        "name",
        "status",
        "conclusion",
        "started_at",
        "completed_at",
        "html_url",
    )
    result = {key: job.get(key) for key in keys}
    result["steps"] = [
        {key: step.get(key) for key in ("name", "status", "conclusion", "number")}
        for step in job.get("steps", [])
    ]
    return result


def public_workflow(workflow):
    keys = ("id", "name", "path", "state", "html_url", "updated_at")
    return {key: workflow.get(key) for key in keys}


def collect_workflows(repository):
    workflows = []
    page = 1
    while True:
        data = api(f"repos/{repository}/actions/workflows?per_page=100&page={page}")
        workflows.extend(public_workflow(item) for item in data["workflows"])
        if len(data["workflows"]) < 100 or len(workflows) >= data["total_count"]:
            return workflows
        page += 1


def collect(repository, max_pages=5, detail_limit=30):
    runs = []
    total = 0
    for page in range(1, max_pages + 1):
        data = api(f"repos/{repository}/actions/runs?per_page=100&page={page}")
        total = data["total_count"]
        batch = data["workflow_runs"]
        runs.extend(public_run(run) for run in batch)
        if len(batch) < 100 or len(runs) >= total:
            break
    warnings = []
    for run in runs[:detail_limit]:
        try:
            attempt = run["run_attempt"] or 1
            data = api(
                f"repos/{repository}/actions/runs/{run['id']}/attempts/{attempt}/jobs?per_page=100"
            )
            run["jobs"] = [public_job(job) for job in data["jobs"]]
            run["job_details_complete"] = data["total_count"] <= len(data["jobs"])
        except (
            OSError,
            requests.RequestException,
            subprocess.CalledProcessError,
            json.JSONDecodeError,
        ):
            # Preserve overview when optional details fail. Do not publish API errors.
            run["jobs"] = []
            run["job_details_complete"] = False
            warnings.append(f"Job details unavailable for run {run['id']}")
    try:
        workflows = collect_workflows(repository)
        workflow_states_available = True
    except (
        OSError,
        requests.RequestException,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ):
        workflows = []
        workflow_states_available = False
        warnings.append("Workflow enablement state unavailable; do not assume active")
    return {
        "schema_version": 1,
        "workflows": workflows,
        "workflow_states_available": workflow_states_available,
        "repository": repository,
        "generated_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 - local Python 3.9 support
        "total_available_runs": total,
        "history_truncated": total > len(runs),
        "detail_limit": detail_limit,
        "warnings": warnings,
        "runs": runs,
    }


def collect_executive(repository):
    """Download only a trusted main-branch observer artifact, then re-project it."""
    data = api(
        f"repos/{repository}/actions/workflows/executive.yml/runs?status=success&branch=main&per_page=10"
    )
    for run in data.get("workflow_runs", []):
        if (
            run.get("head_branch") != "main"
            or run.get("path") != ".github/workflows/executive.yml"
            or (run.get("head_repository") or {}).get("full_name") != repository
        ):
            continue
        artifacts = api(f"repos/{repository}/actions/runs/{run['id']}/artifacts")
        if not any(
            a.get("name") == "mail-lake-executive-public" and not a.get("expired")
            for a in artifacts.get("artifacts", [])
        ):
            continue
        with tempfile.TemporaryDirectory(prefix="mail-lake-public-") as directory:
            subprocess.run(
                [
                    "gh",
                    "run",
                    "download",
                    str(run["id"]),
                    "--repo",
                    repository,
                    "--name",
                    "mail-lake-executive-public",
                    "--dir",
                    directory,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            path = Path(directory) / "executive-public.json"
            if path.stat().st_size > 262144:
                raise ValueError("Public executive artifact exceeds size limit")
            return sanitize_public(json.loads(path.read_text()))
    raise ValueError("No trusted executive artifact available")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repository",
        default=os.environ.get("GITHUB_REPOSITORY", "considerable/mail-parquet-lake"),
    )
    parser.add_argument("--output", default="dashboard/data.json")
    parser.add_argument("--executive-output", default="dashboard/executive.json")
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--job-details", type=int, default=30)
    args = parser.parse_args()
    data = collect(args.repository, args.max_pages, args.job_details)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, indent=2) + "\n")
    try:
        executive = collect_executive(args.repository)
    except (
        OSError,
        requests.RequestException,
        subprocess.CalledProcessError,
        ValueError,
        KeyError,
        TypeError,
    ):
        executive = {"schema_version": 1, "available": False, "observed_at": None}
    Path(args.executive_output).write_text(
        json.dumps(executive, allow_nan=False, indent=2) + "\n"
    )
    print(
        f"Exported {len(data['runs'])} public-safe runs and executive evidence to {output.parent}"
    )


if __name__ == "__main__":
    main()
