"""Create a Pipeline-from-SCM job on a running Jenkins, trigger it and wait for the result.

    python ci/run_jenkins_job.py --url http://localhost:8080 --repo file:///repo

Exits non-zero (after printing the console log) unless the build finishes SUCCESS.
"""

import argparse
import http.cookiejar
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

JOB_XML = """<?xml version='1.1' encoding='UTF-8'?>
<flow-definition plugin="workflow-job">
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition" plugin="workflow-cps">
    <scm class="hudson.plugins.git.GitSCM" plugin="git">
      <configVersion>2</configVersion>
      <userRemoteConfigs><hudson.plugins.git.UserRemoteConfig><url>{repo}</url></hudson.plugins.git.UserRemoteConfig></userRemoteConfigs>
      <branches><hudson.plugins.git.BranchSpec><name>*/main</name></hudson.plugins.git.BranchSpec></branches>
    </scm>
    <scriptPath>Jenkinsfile</scriptPath>
    <lightweight>false</lightweight>
  </definition>
</flow-definition>
"""


# One cookie-carrying session: Jenkins ties the CSRF crumb to the session that fetched it.
OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))


def call(url: str, data: bytes | None = None, headers: dict | None = None):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method="POST" if data is not None else "GET")
    return OPENER.open(req, timeout=60)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--repo", required=True)
    ap.add_argument("--timeout", type=int, default=900)
    args = ap.parse_args()
    base = args.url.rstrip("/")

    headers = {"Content-Type": "application/xml"}
    try:  # CSRF crumb, if the controller issues one
        crumb = json.load(call(f"{base}/crumbIssuer/api/json"))
        headers[crumb["crumbRequestField"]] = crumb["crumb"]
    except urllib.error.HTTPError:
        pass

    call(f"{base}/createItem?name=quotes", JOB_XML.format(repo=args.repo).encode(), headers)
    call(f"{base}/job/quotes/build", b"", {k: v for k, v in headers.items() if k != "Content-Type"})
    print("build triggered", flush=True)

    deadline = time.time() + args.timeout
    result = None
    while time.time() < deadline:
        try:
            info = json.load(call(f"{base}/job/quotes/lastBuild/api/json"))
        except urllib.error.HTTPError:
            time.sleep(3)
            continue
        if not info["building"]:
            result = info["result"]
            break
        time.sleep(5)

    log = call(f"{base}/job/quotes/lastBuild/consoleText").read().decode(errors="replace")
    print(log[-6000:])
    print(f"result: {result}")
    return 0 if result == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
