"""Hosted-only SH1 lanes. Upload only the explicitly listed JSON reports."""
import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import time
import zipfile

MODULE = "github.com/relux-works/swarma-session-host"
TEST_COMMAND = ["go", "test", "-json", "-count=1", "-timeout=12m",
                "./internal/sessionmanager", "./internal/sessionmanager/musepty"]
REPORTS = {"summary.json", "manifest.json", "project.json", "inventory.json",
           "baseline-inventory.json", "equivalence.json", "guards.json",
           "mutations.json", "consumer.json"}


def source_setting(environment, name, module=False):
    value = environment.get(name, "")
    if not value.strip():
        raise ValueError(name + " must be set")
    path = PurePosixPath(value)
    if (path.is_absolute() or path.as_posix() != value or ".." in path.parts
            or not re.fullmatch(r"[A-Za-z0-9._/-]+", value) or value == "."):
        raise ValueError(name + " must be a safe relative path")
    if module and ("." not in path.parts[0] or not re.fullmatch(r"[a-z0-9._/-]+", value)):
        raise ValueError(name + " must be a supported module path")
    return value


def source_configuration(environment):
    return (source_setting(environment, "PRIVATE_SOURCE_CLI_DIR"),
            source_setting(environment, "PRIVATE_SOURCE_MODULE", module=True))


def configured_mutations(path, source_module):
    module = source_setting({"PRIVATE_SOURCE_MODULE": source_module}, "PRIVATE_SOURCE_MODULE", module=True)
    declaration = json.loads(path.read_text())
    for mutation in declaration["mutations"]:
        relative = source_setting(mutation, "package")
        mutation["package"] = module + "/" + relative
    return declaration


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def outcome_set(inventory, package):
    """Refuse unknown evidence; intentional skips and tagged absence are outcomes."""
    packages = {p["package"]: p for p in inventory["packages"]}
    if len(packages) != len(inventory["packages"]):
        raise ValueError("duplicate inventory package")
    tested = {package, package + "/musepty"}
    if not tested.issubset(packages):
        raise ValueError("inventory must contain both tested packages")
    outcomes = {}
    for name in sorted(packages):
        row = packages[name]
        # Recursive discovery retains fixture declarations. Go's unchanged
        # two-package command does not select packages below testdata.
        fixture = name.startswith((package + "/testdata/", package + "/musepty/testdata/"))
        if name not in tested and not fixture:
            raise ValueError("unexpected inventory package: " + name)
        if row["status"] not in (("not_observed",) if fixture else ("pass", "fail")):
            raise ValueError("package outcome is unknown: " + name)
        outcomes[(name, "")] = {"status": row["status"], "scope": "fixture" if fixture else "tested"}
        tests = row["tests"]
        if not tests or (not fixture and not any(t.get("executed") for t in tests)):
            raise ValueError("no tests executed: " + name)
        for test in tests:
            status = test["status"]
            tagged_absence = status == "not_observed" and test.get("tags") and not test.get("executed")
            fixture_absence = fixture and status == "not_observed" and not test.get("executed") and test.get("file")
            if fixture and not fixture_absence:
                raise ValueError("unexpected fixture execution: " + name + "/" + test["name"])
            if not fixture_absence and not tagged_absence and (status not in ("pass", "fail", "skip") or not test.get("executed")):
                raise ValueError("unknown test outcome: " + name + "/" + test["name"])
            if status == "skip" and test.get("reason", "unknown") in ("", "unknown"):
                raise ValueError("skip reason is unknown: " + name + "/" + test["name"])
            key = (name, test["name"])
            if key in outcomes:
                raise ValueError("duplicate test outcome: " + name + "/" + test["name"])
            outcomes[key] = {field: test.get(field, "") for field in
                             ("status", "executed", "file", "tags", "opt_in")}
            if status == "skip":
                outcomes[key]["reason"] = test["reason"]
    return outcomes


def compare_inventories(baseline, projection, baseline_exit, projection_exit, package):
    before, after = outcome_set(baseline, package), outcome_set(projection, package)
    differences = [{"package": key[0], "test": key[1], "baseline": before.get(key),
                    "projection": after.get(key)}
                   for key in sorted(before.keys() | after.keys()) if before.get(key) != after.get(key)]
    return {"version": 1, "equivalent": not differences and baseline_exit == projection_exit,
            "baseline_exit": baseline_exit, "projection_exit": projection_exit,
            "outcomes_compared": len(before), "differences": differences}


def counts(inventory, package):
    tests = [t for p in inventory["packages"]
             if p["package"] in (package, package + "/musepty") for t in p["tests"]]
    return {"discovered": sum(bool(t.get("file")) for t in tests),
            "dynamic_subtests": sum(not bool(t.get("file")) for t in tests),
            "executed": sum(bool(t.get("executed")) for t in tests),
            "skipped": sum(t["status"] == "skip" for t in tests),
            "failed": sum(t["status"] == "fail" for t in tests),
            "tagged": sum(bool(t.get("tags")) for t in tests),
            "opt_in": sum(bool(t.get("opt_in")) for t in tests),
            "not_observed": sum(t["status"] == "not_observed" for t in tests)}


def inventory_source(checkout, destination):
    """Inventory the two tested packages at their original module-relative paths."""
    destination.mkdir()
    shutil.copyfile(checkout / "go.mod", destination / "go.mod")
    shutil.copytree(checkout / "internal/sessionmanager", destination / "internal/sessionmanager")
    return destination


def publish_local_module(root, proxy):
    """Declared retained board libraries, not a replacement of the candidate."""
    module = root.joinpath("go.mod").read_text().splitlines()[0].split()[1]
    version = "v0.0.0"
    dest = proxy / module / "@v"
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(root / "go.mod", dest / (version + ".mod"))
    write(dest / (version + ".info"), {"Version": version, "Time": "2026-10-10T00:00:00Z"})
    (dest / "list").write_text(version + "\n")
    with zipfile.ZipFile(dest / (version + ".zip"), "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(root.rglob("*")):
            if ".git" in path.parts:
                continue
            if path.is_symlink():
                raise RuntimeError("local module contains a symlink")
            if path.is_file():
                archive.write(path, module + "@" + version + "/" + path.relative_to(root).as_posix())
    return dest.parent


def retained_library_report(proxy, modules):
    return {str(p.relative_to(proxy)): sha(p) for module in modules
            for p in sorted(module.rglob("*")) if p.is_file()}


class Lane:
    def __init__(self):
        self.cli_dir, self.source_module = source_configuration(os.environ)
        self.package = self.source_module + "/internal/sessionmanager"
        self.host = Path("host").resolve()
        self.board = Path("board").resolve()
        self.checkout = self.board / self.cli_dir
        self.temp = Path(os.environ["RUNNER_TEMP"]) / "sh1-work"
        self.temp.mkdir()
        self.reports = self.temp.parent / "sh1-reports"
        self.reports.mkdir()
        self.tool = self.temp / "sh1"
        self.candidate = self.temp / "candidate"
        self.summary = {"lane": os.environ["SH1_LANE"], "board_commit": os.environ["SH1_BOARD_COMMIT"],
                        "commands": [], "failures": [], "run_id": os.environ["GITHUB_RUN_ID"],
                        "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
                        "runner_os": os.environ["RUNNER_OS"], "build_tags": [],
                        "opt_in_environment": {}, "adapters": "empty version-1 table",
                        "gating": os.environ["SH1_LANE"] != "projection" or os.environ["RUNNER_OS"] == "macOS"}
        self.started = time.monotonic()

    def command(self, args, cwd=None, env=None, required=True, events=None):
        args = [str(a) for a in args]
        started = time.monotonic()
        print("RUN", self.safe(" ".join(args)), flush=True)
        result = subprocess.run(args, cwd=cwd or self.host, env=env, capture_output=True)
        # Raw output can contain board code; it stays in job logs, never artifacts.
        out = result.stdout.decode(errors="replace")
        err = result.stderr.decode(errors="replace")
        row = {"command": [self.safe(a) for a in args], "exit": result.returncode,
               "duration_seconds": round(time.monotonic() - started, 3)}
        self.summary["commands"].append(row)
        print("EXIT", result.returncode, flush=True)
        if events:
            events.write_bytes(result.stdout)
            print(self.safe(out), end="")
            failures = []
            for line in out.splitlines():
                ev = json.loads(line)
                if ev.get("Action") in ("fail", "skip"):
                    failures.append({k: ev[k] for k in ("Action", "Package", "Test") if k in ev})
            self.summary.setdefault("test_terminals", {})[events.stem] = failures
        else:
            print(self.safe(out), end="")
        print(self.safe(err), end="", file=sys.stderr)
        if result.returncode and required:
            self.summary["failures"].append(row)
            raise RuntimeError("command refused: " + " ".join(row["command"]))
        return result

    def safe(self, text):
        for path, label in ((self.host, "$HOST"), (self.board, "$BOARD"),
                            (self.temp, "$WORK"), (self.temp.parent, "$RUNNER_TEMP")):
            text = text.replace(str(path), label)
        return text

    def prepare(self):
        actual = self.command(["git", "rev-parse", "HEAD"], self.board).stdout.decode().strip()
        if actual != self.summary["board_commit"]:
            raise RuntimeError("board checkout identity differs from resolved commit")
        self.summary["host_commit"] = self.command(["git", "rev-parse", "HEAD"]).stdout.decode().strip()
        self.summary["host_tree"] = self.command(["git", "rev-parse", "HEAD^{tree}"]).stdout.decode().strip()
        self.summary["go_version"] = self.command(["go", "version"]).stdout.decode().strip()
        self.summary["go_environment"] = self.command(["go", "env", "GOOS", "GOARCH", "GOVERSION", "GOTOOLCHAIN"]).stdout.decode().splitlines()
        self.summary["helper_hashes"] = {p.name: sha(p) for p in sorted((self.host / "tools/sh1/ci").glob("*")) if p.is_file()}
        self.summary["workflow_hash"] = sha(self.host / ".github/workflows/sh1-lanes.yml")
        self.command(["go", "build", "-o", self.tool, "./tools/sh1"])
        mod = self.temp / "candidate-go.mod"
        shutil.copyfile(self.checkout / "go.mod", mod)
        parsed = json.loads(self.command(["go", "mod", "edit", "-json"], self.checkout).stdout)
        args = ["go", "mod", "edit", "-module=" + MODULE]
        for replace in parsed.get("Replace") or []:
            old = replace["Old"]
            args.append("-dropreplace=" + old["Path"] + ("@" + old["Version"] if old.get("Version") else ""))
        self.command(args + [mod])
        self.command([self.tool, "generate", "-source", self.checkout,
                      "-package", "internal/sessionmanager", "-old", self.package,
                      "-new", MODULE, "-mod", mod, "-out", self.candidate])
        shutil.copyfile(self.candidate / "sh1-manifest.json", self.reports / "manifest.json")
        manifest = json.loads((self.reports / "manifest.json").read_text())
        self.summary.update({"generator_digest": manifest["generator"],
                             "transform_digest": manifest["transform"],
                             "manifest_hash": sha(self.reports / "manifest.json"),
                             "source_go_mod_hash": manifest["source_mod"],
                             "candidate_go_mod_hash": sha(mod)})

    def projection(self):
        # The guard expectation is derived from the unchanged guard's source scope.
        targets = sorted(p.name for p in (self.checkout / "internal/sessionmanager").glob("claude*.go")
                         if not p.name.endswith("_test.go"))
        declaration = {"version": 1, "guards": [
            {"package": self.package, "test": "TestClaudeContextProductionHasNoFilesystemRemoval", "targets": targets},
            {"package": self.package, "test": "TestTheMutantInventoryCoversEveryPredicateOnTheRefusalPath",
             "targets": ["hosted_takeover_guard.go"]}],
            "opt_in": [
                {"package": self.package, "test": name, "condition": condition}
                for name, condition in (
                    ("TestClaudeHostLiveSmoke", "TASK_BOARD_CLAUDE_LIVE_SMOKE=1 and trusted project and Claude binary"),
                    ("TestClaudeAdoptionLiveSmoke", "TASK_BOARD_CLAUDE_LIVE_SMOKE=1 and trusted project and Claude binary"),
                    ("TestMuseRealProviderBudgetPause", "TASK_BOARD_MUSE_REAL_PROVIDER=1 and qualified TASK_BOARD_MUSE_SMOKE_BIN"),
                )]}
        guards = self.temp / "guard-declaration.json"
        write(guards, declaration)
        self.summary["guard_declaration_hash"] = sha(guards)
        baseline, baseline_exit = self.test_inventory("baseline", guards, "baseline-inventory.json")
        self.command([self.tool, "project", "-candidate", self.candidate,
                      "-checkout", self.checkout, "-package", "internal/sessionmanager",
                      "-commit", self.summary["board_commit"], "-adapters",
                      self.host / "tools/sh1/ci/adapters.json", "-report", self.reports / "project.json"])
        inventory, projection_exit = self.test_inventory("projection", guards, "inventory.json")
        diff = compare_inventories(baseline, inventory, baseline_exit, projection_exit, self.package)
        write(self.reports / "equivalence.json", diff)
        self.summary["equivalence"] = diff
        if not diff["equivalent"]:
            self.summary["failures"].append({"gate": "baseline equivalence", "differences": diff["differences"],
                                             "baseline_exit": baseline_exit, "projection_exit": projection_exit})
        write(self.reports / "guards.json", {"guard_ratio": inventory["guard_ratio"],
                                           "guards": inventory["guards"], "gating": False,
                                           "bound": "not instrumented upstream"})
        self.summary["guard_ratio"] = inventory["guard_ratio"]
        if self.summary["runner_os"] != "macOS":
            self.summary["mutations"] = "not run: mutations gate on macOS only"
            return
        mutations = self.temp / "mutations.json"
        write(mutations, configured_mutations(self.host / "tools/sh1/ci/mutations.json", self.source_module))
        result = self.command([self.tool, "mutate", "-checkout", self.checkout,
                               "-mutations", mutations,
                               "-report", self.reports / "mutations.json", "--"] + TEST_COMMAND, required=False)
        if result.returncode:
            self.summary["failures"].append({"gate": "mutations", "exit": result.returncode, "status": "unproven unless report records kills"})
        if (self.reports / "mutations.json").exists():
            self.summary["mutation_ratio"] = json.loads((self.reports / "mutations.json").read_text())["kill_ratio"]

    def test_inventory(self, label, guards, report_name):
        events = self.temp / (label + ".jsonl")
        result = self.command(TEST_COMMAND, self.checkout, required=False, events=events)
        source = inventory_source(self.checkout, self.temp / (label + "-inventory-source"))
        report = self.reports / report_name
        measured = self.command([self.tool, "inventory", "-source", source,
                                 "-events", events, "-guards", guards, "-report", report], required=False)
        # The CLI writes a validated inventory before its guard-coverage refusal.
        # Missing/malformed evidence never becomes an informative coverage gap.
        inventory = json.loads(report.read_text())
        outcome_set(inventory, self.package)
        if measured.returncode not in (0, 1) or (measured.returncode == 1 and all(
                g["reached"] == g["expected"] for g in inventory["guards"])):
            raise RuntimeError("inventory refused beyond informative guard coverage")
        self.summary[label + "_inventory_counts"] = counts(inventory, self.package)
        self.summary[label + "_failed_tests"] = [
            {"package": p["package"], "test": t["name"]}
            for p in inventory["packages"] for t in p["tests"] if t["status"] == "fail"]
        return inventory, result.returncode

    def consumer(self):
        # Stage all upstream dependency bytes first, then forbid network fallback.
        cache = self.temp / "upstream-cache"
        env = dict(os.environ, GOMODCACHE=str(cache), GOPROXY="https://proxy.golang.org", GOSUMDB="sum.golang.org")
        self.command(["go", "mod", "download", "all"], self.checkout, env)
        proxy = self.temp / "proxy"
        shutil.copytree(cache / "cache/download", proxy)
        libraries = [publish_local_module(self.board / "pkg" / lib, proxy)
                     for lib in ("board", "remoteconfig")]
        self.summary["retained_library_proxy"] = retained_library_report(proxy, libraries)
        # Dependency exit: these tests need board-internal access and stay in lane 1.
        shutil.rmtree(self.checkout / "internal/sessionmanager")
        stamp = self.command(["git", "show", "-s", "--format=%ct", "HEAD"]).stdout.decode().strip()
        import datetime
        timestamp = datetime.datetime.fromtimestamp(int(stamp), datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
        version = "v0.0.0-" + timestamp + "-" + self.summary["host_commit"][:12]
        result = self.command([self.tool, "consumer", "-candidate", self.candidate,
                               "-checkout", self.checkout, "-version", version,
                               "-proxy", proxy, "-report", self.reports / "consumer.json"], required=False)
        if result.returncode:
            self.summary["failures"].append({"gate": "consumer proxy selection", "exit": result.returncode})
        env = dict(os.environ, GOPROXY=proxy.as_uri(), GOSUMDB="off", GOPRIVATE="", GONOPROXY="none",
                   GONOSUMDB="none", GOMODCACHE=str(self.temp / "build-cache"), GOFLAGS="-mod=mod")
        selection = json.loads(self.command(["go", "mod", "edit", "-json"], self.checkout).stdout)
        if any(r["Old"]["Path"] == MODULE for r in selection.get("Replace") or []):
            raise RuntimeError("candidate replace found before consumer build")
        for args in (["go", "build", "./..."], ["go", "vet", "./..."]):
            result = self.command(args, self.checkout, env, required=False)
            if result.returncode:
                self.summary["failures"].append({"gate": " ".join(args), "exit": result.returncode})

    def finish(self):
        self.summary["duration_seconds"] = round(time.monotonic() - self.started, 3)
        self.summary["report_hashes"] = {p.name: sha(p) for p in sorted(self.reports.iterdir()) if p.is_file()}
        write(self.reports / "summary.json", self.summary)
        if set(p.name for p in self.reports.iterdir()) - REPORTS:
            raise RuntimeError("unexpected artifact file")


def main():
    try:
        lane = Lane()
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    try:
        lane.prepare()
        if lane.summary["lane"] == "projection":
            lane.projection()
        elif lane.summary["lane"] == "consumer":
            lane.consumer()
        else:
            raise RuntimeError("unknown lane")
    except Exception as error:
        lane.summary["failures"].append({"error": lane.safe(str(error))})
        print(lane.safe(str(error)), file=sys.stderr)
    finally:
        lane.finish()
    return 1 if lane.summary["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
