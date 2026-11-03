# Workflow Observatory

A static GitHub Pages dashboard built from a **public-safe allowlist** of GitHub Actions metadata. It collects no raw logs, email data, reports, commit messages, inputs, or artifacts.

- Updates when Gmail Sync, Job Tracker, or Security & Code Quality completes, on dashboard source changes, on manual dispatch, and approximately every 30 minutes. Schedules are best effort.
- Fetches up to 500 latest run records across all workflows, with job/step details for the latest 30. The displayed coverage is explicit. GitHub returns each run's latest attempt; this is not a complete audit of every earlier rerun attempt.
- The timeline represents actual runs, not scheduled slots. No run is never treated as success. Independent missed-run detection and application/data-freshness metrics are future work.
- Duration is approximate workflow elapsed time from `run_started_at` to `updated_at`, not CPU time or exact job duration.
- A stale-snapshot warning appears after 90 minutes. Refresh reloads the published snapshot; it does not trigger a workflow. A failed collector does not replace the last published site with empty/healthy data.
- Retention depends on GitHub's available history. For a durable archive beyond that, add a separately governed metadata store. Do not place email contents on Pages.

## Local preview

Run `python scripts/build_dashboard.py` with GitHub CLI authentication or a read-only `GH_TOKEN` / `GITHUB_TOKEN` (never put tokens in browser code). Then serve `dashboard/` using `python -m http.server 8765 --directory dashboard`. The generated `data.json` is ignored by Git and included only in the Pages artifact.

## Deployment

Enable Pages with **GitHub Actions** as the build source. `.github/workflows/dashboard.yml` builds and deploys the site. Only the deploy job has Pages write and OIDC permissions; it has no AWS permissions. The collector has Actions read access and checks out the trusted main branch, including for `workflow_run` events. It never executes triggering PR code or downloads its artifacts.

Check **Actions → Dashboard** if the page becomes stale. Existing sync/tracker workflows and AWS resources are unchanged.

## Workflow eligibility is a separate status factor

The collector queries the workflow API as well as run history. The page distinguishes enabled, disabled for inactivity, manually disabled, fork-disabled, deleted, and unknown states. An API failure is unknown, never implicitly enabled. Current enablement can be disabled even when the last execution succeeded, and an enabled workflow can have a failed last run.

GitHub automatically disables scheduled workflows in public repositories after 60 days without repository activity. Recurring workflow executions do not by themselves establish repository activity. A recent commit should not be assumed to re-enable an already-disabled workflow: verify the API state. Re-enable deliberately in Actions after investigating execution failures. This dashboard does not enable or rerun any workflow.

Source: https://docs.github.com/en/actions/how-tos/manage-workflow-runs/disable-and-enable-workflows

Private email notices helped identify the cause, but email content, addresses, and message IDs are not published. The displayed reasons come from GitHub's public API state and documented behavior. API `updated_at` is not labeled as an exact disable timestamp. Billing restrictions are not inferred from absence of runs or from generic failures.
