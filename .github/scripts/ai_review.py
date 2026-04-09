"""
AI code review via Amazon Bedrock Nova Micro.
Reads a git diff, sends to Bedrock, prints findings as markdown.
"""

import json
import os
import sys
import boto3

MODEL_ID = "us.amazon.nova-micro-v1:0"
PROMPT = """You are a senior security-focused code reviewer.
Review this git diff for:
- Security vulnerabilities (injection, credential leaks, path traversal)
- Bugs and logic errors
- Python best practices violations

Be concise. Only report real issues, not style nits.
If no issues found, say "No issues found."

Diff:
```
{diff}
```"""


def review(diff_path: str):
    with open(diff_path) as f:
        diff = f.read()

    if len(diff) > 50000:
        diff = diff[:50000] + "\n... (truncated)"

    client = boto3.client("bedrock-runtime", region_name="us-west-2")
    resp = client.converse(
        modelId=MODEL_ID,
        messages=[{"role": "user", "content": [{"text": PROMPT.format(diff=diff)}]}],
    )
    review_text = resp["output"]["message"]["content"][0]["text"]

    # Write to Job Summary
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as f:
            f.write("## AI Code Review (Bedrock Nova Micro)\n\n")
            f.write(review_text + "\n")

    # Post as PR comment
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    pr_number = os.environ.get("PR_NUMBER")
    if token and repo and pr_number:
        import urllib.request

        body = f"## 🤖 AI Code Review (Bedrock Nova Micro)\n\n{review_text}"
        data = json.dumps({"body": body}).encode()
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments",
            data=data,
            headers={
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json",
            },
        )
        # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
        urllib.request.urlopen(req)

    print(review_text)


if __name__ == "__main__":
    review(sys.argv[1])
