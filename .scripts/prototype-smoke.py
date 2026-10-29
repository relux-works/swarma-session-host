#!/usr/bin/env python3
"""Exercise only synthetic children; never launch a real harness or read credentials."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time


def rpc(endpoint, request):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(3)
        connection.connect(str(endpoint))
        connection.sendall(json.dumps(request).encode() + b"\n")
        with connection.makefile("rb") as stream:
            line = stream.readline(65537)
        if len(line) > 65536:
            raise AssertionError("oversized response")
        return json.loads(line)


def wait_for(predicate, label):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.01)
    raise AssertionError(label)


def lane(binary_dir, report_dir, protocol):
    processes = []
    logs = []
    result = {"protocol": protocol, "passed": False, "owned_pids": []}
    try:
        with tempfile.TemporaryDirectory(prefix="session-smoke-", dir="/tmp") as temporary:
            root = Path(temporary)
            os.chmod(root, 0o700)
            runner_socket = root / "runner.sock"
            state_dir = root / "state"
            execution = "fixture-execution"

            def start(name, command, plan=None):
                log = open(report_dir / f"{protocol}-{name}.log", "wb")
                logs.append(log)
                process = subprocess.Popen(command, stdin=subprocess.PIPE if plan else subprocess.DEVNULL,
                                           stdout=log, stderr=log, env={"PATH": "/usr/bin:/bin"})
                processes.append(process)
                result["owned_pids"].append({"role": name, "pid": process.pid})
                if plan:
                    process.stdin.write(json.dumps(plan).encode())
                    process.stdin.close()
                return process

            runner_process = start("runner", [str(binary_dir / "swarma-session-runner"), "-socket", str(runner_socket),
                                               "-execution", execution, "-io-timeout", "1s"],
                                   {"binary": str(binary_dir / "session-fixture"), "args": ["-protocol", protocol],
                                    "cwd": temporary, "env": []})
            wait_for(lambda: runner_socket.exists() and runner_process.poll() is None, "runner not ready")

            def start_host(name):
                endpoint = root / f"{name}.sock"
                process = start(name, [str(binary_dir / "swarma-session-host"), "serve", "-socket", str(endpoint),
                                       "-runner", str(runner_socket), "-state", str(state_dir), "-execution", execution,
                                       "-adapter", "json-fixture" if protocol == "json" else "line-fixture",
                                       "-vendor", "unlisted-vendor", "-model", "future-model@7", "-reasoning", "new-effort",
                                       "-input-state", "idle", "-io-timeout", "2s"])
                wait_for(lambda: endpoint.exists() and process.poll() is None, "host not ready")
                return process, endpoint

            def applied(endpoint, identifier):
                snapshot = rpc(endpoint, {"op": "status"})["state"]
                return any(record["notice"]["id"] == identifier and record["state"] == "applied"
                           for record in snapshot["records"] or [])

            first_host, first_endpoint = start_host("host-one")
            first = {"op": "enqueue", "execution": execution, "epoch": 1,
                     "notice": {"id": "one", "reference": "fixture/one"}}
            assert rpc(first_endpoint, first)["code"] == "ok", "first enqueue refused"
            wait_for(lambda: applied(first_endpoint, "one"), "first fixture ack missing")
            before = rpc(runner_socket, {"op": "status", "execution": execution, "epoch": 0})

            # Kill the exact host process we started. The fixture child belongs
            # to the separately started runner and must remain the same process.
            first_host.kill()
            first_host.wait(timeout=3)
            _, second_endpoint = start_host("host-two")
            after = rpc(runner_socket, {"op": "status", "execution": execution, "epoch": 0})
            assert (before["pid"], before["instance"]) == (after["pid"], after["instance"]), "child replaced on host crash"
            assert not after["exited"], "host crash killed child"
            assert rpc(second_endpoint, first)["code"] == "stale_session", "stale host epoch accepted"
            first["epoch"] = 2
            assert rpc(second_endpoint, first)["code"] == "ok", "exact retry lost receipt"
            first["notice"]["reference"] = "changed-intent"
            assert rpc(second_endpoint, first)["code"] == "request_conflict", "changed intent accepted"
            second = {"op": "enqueue", "execution": execution, "epoch": 2,
                      "notice": {"id": "two", "reference": "fixture/two"}}
            assert rpc(second_endpoint, second)["code"] == "ok", "second enqueue refused"
            wait_for(lambda: applied(second_endpoint, "two"), "second fixture ack missing")
            stopped = rpc(second_endpoint, {"op": "stop", "execution": execution, "epoch": 2})
            assert stopped["code"] == "ok" and stopped["runner"]["exited"], "owned child stop unconfirmed"
            assert stopped["runner"]["writes"] == 2, "a forbidden duplicate write occurred"
            assert rpc(second_endpoint, {"op": "release", "execution": execution, "epoch": 2})["code"] == "session_stopped", "stop reopened"
            result.update(passed=True, child_pid=before["pid"], runner_instance=before["instance"],
                          writes=2, host_kill_exit=first_host.returncode, stopped_exit=stopped["runner"]["exit_code"])
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        for process, receipt in reversed(list(zip(processes, result["owned_pids"]))):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            receipt["exit_code"] = process.returncode
            expected_crash = receipt["role"] == "host-one" and process.returncode == -9
            if result["passed"] and process.returncode != 0 and not expected_crash:
                result.update(passed=False, error=f"owned cleanup failed: {receipt['role']}")
        for log in logs:
            log.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--report-dir", required=True, type=Path)
    args = parser.parse_args()
    binary_dir = args.bin_dir.resolve(strict=True)
    args.report_dir.mkdir(parents=True, exist_ok=True)
    # Every selected protocol runs, even if the first lane fails.
    results = [lane(binary_dir, args.report_dir, protocol) for protocol in ("json", "line")]
    report = {"fixture_only": True, "lanes": results}
    (args.report_dir / "smoke.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
