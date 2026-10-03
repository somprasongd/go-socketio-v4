"""Require successful Go packages and the real-client/Redis acceptance tests."""
import json
import sys
from pathlib import Path

REQUIRED = {
    ("github.com/somprasongd/go-socketio-v4", "TestJSInterop"),
    ("github.com/somprasongd/go-socketio-v4/redisstreamsadapter", "TestDistributedJSRecovery"),
    ("github.com/somprasongd/go-socketio-v4/redisstreamsadapter", "TestRealRedisDisconnectResumeAndFailClosed"),
}


def check_events(events):
    passed = set()
    failures = []
    for event in events:
        if event.get("Action") == "fail":
            failures.append(event.get("Test") or event.get("Package") or "unknown failure")
        if event.get("Action") == "pass":
            passed.add((event.get("Package"), event.get("Test")))
    errors = []
    if failures:
        errors.append("Go tests failed: " + ", ".join(failures))
    missing = REQUIRED - passed
    if missing:
        errors.append("Required tests did not pass: " + ", ".join(sorted(test for _, test in missing)))
    return errors


def main(path):
    events = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    errors = check_events(events)
    if errors:
        raise SystemExit("\n".join(errors))
    print("All Go tests and required JavaScript/Redis acceptance tests passed.")


if __name__ == "__main__":
    main(sys.argv[1])
