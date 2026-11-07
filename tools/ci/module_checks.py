"""Run the hosted module checks without reporting admission refusals as test exits."""

import json
import os
import subprocess
import time


CHECKS = (
    ("vet", ("go", "vet", "-p", "1", "./...")),
    ("race", ("go", "test", "-p", "1", "-race", "-count=1", "-timeout=5m", "./...")),
)
LOAD_LIMIT = 20
WAIT_SECONDS = 120
POLL_SECONDS = 5


def admit(loadavg=os.getloadavg, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + WAIT_SECONDS
    while True:
        try:
            load = loadavg()[0]
        except OSError as error:
            print("load admission unavailable:", error, flush=True)
            return False
        print("start load 1m:", load, flush=True)
        if load < LOAD_LIMIT:
            return True
        remaining = deadline - clock()
        if remaining <= 0:
            return False
        sleep(min(POLL_SECONDS, remaining))


def run_checks(admission=admit, run=subprocess.call):
    failed = unavailable = False
    for name, command in CHECKS:
        result = {"check": name, "command": list(command), "exit_code": None}
        if not admission():
            unavailable = True
            result.update(status="unavailable", reason="load_admission_refused",
                          admission_exit_code=125)
            print("::warning::%s not run: load admission refused within %ss"
                  % (name, WAIT_SECONDS), flush=True)
        else:
            try:
                code = run(command)
            except OSError as error:
                unavailable = True
                result.update(status="unavailable", reason="check_start_failed",
                              error=str(error))
            else:
                failed |= code != 0
                result.update(status="pass" if code == 0 else "fail", exit_code=code)
        print(json.dumps(result, sort_keys=True), flush=True)
    # A product failure takes precedence over a concurrent infrastructure refusal.
    code = 1 if failed else 125 if unavailable else 0
    print(json.dumps({"overall": "fail" if failed else "unavailable" if unavailable else "pass",
                      "exit_code": code}, sort_keys=True), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(run_checks())
