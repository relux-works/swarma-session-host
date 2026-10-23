"""Configuration controls use invented source identities, never private defaults."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("configured_lane", Path(__file__).with_name("run.py"))
lane = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lane)

SOURCE = {"PRIVATE_SOURCE_CLI_DIR": "tools/source-cli",
          "PRIVATE_SOURCE_MODULE": "example.org/source-cli"}
WORKFLOW_SOURCE = dict(SOURCE, PRIVATE_SOURCE_REPO="example/source")


class SourceConfigurationTests(unittest.TestCase):
    # Both supplied fields must determine checkout/package identity before any workspace writes.
    def test_configuration_controls_checkout_and_import_identity(self):
        for settings in (SOURCE, {"PRIVATE_SOURCE_CLI_DIR": "different/cli",
                                  "PRIVATE_SOURCE_MODULE": "example.net/alternate"}):
            with self.subTest(settings=settings), tempfile.TemporaryDirectory() as directory:
                environment = dict(settings, RUNNER_TEMP=directory, SH1_LANE="projection",
                                   SH1_BOARD_COMMIT="a" * 40, GITHUB_RUN_ID="1",
                                   GITHUB_RUN_ATTEMPT="1", RUNNER_OS="Linux")
                with patch.dict(os.environ, environment, clear=True):
                    instance = lane.Lane()
                self.assertEqual(instance.checkout, Path("board").resolve() / settings["PRIVATE_SOURCE_CLI_DIR"])
                self.assertEqual(instance.package, settings["PRIVATE_SOURCE_MODULE"] + "/internal/sessionmanager")
                self.assertTrue(instance.temp.is_dir())

    # A missing/empty setting must refuse entry with its name, without subprocesses or filesystem writes.
    def test_missing_source_values_refuse_before_side_effects(self):
        for name in SOURCE:
            for value in (None, "", " \t"):
                with self.subTest(name=name, value=value), tempfile.TemporaryDirectory() as directory:
                    environment = dict(SOURCE, RUNNER_TEMP=directory)
                    if value is None:
                        environment.pop(name)
                    else:
                        environment[name] = value
                    error = io.StringIO()
                    with patch.dict(os.environ, environment, clear=True), \
                            patch.object(lane.subprocess, "run") as command, \
                            contextlib.redirect_stderr(error):
                        self.assertEqual(lane.main(), 1)
                    self.assertIn(name + " must be set", error.getvalue())
                    command.assert_not_called()
                    self.assertEqual(list(Path(directory).iterdir()), [])

    # Directory/module settings cannot traverse out of the checkout or produce malformed import identities.
    def test_unsafe_source_values_refuse(self):
        for name, invalid in (("PRIVATE_SOURCE_CLI_DIR", ("../escape", "/absolute", ".", "a/../b", "a\\b", "a\nb")),
                              ("PRIVATE_SOURCE_MODULE", ("../escape", "no-domain", "example.org/a b", "example.org/a/../b"))):
            for value in invalid:
                with self.subTest(name=name, value=value):
                    with self.assertRaisesRegex(ValueError, name):
                        lane.source_configuration(dict(SOURCE, **{name: value}))

    # Relative mutation declarations bind all named tests to the configured module, with no source edits.
    def test_mutations_use_explicit_module_and_keep_other_fields(self):
        path = Path(__file__).with_name("mutations.json")
        original = json.loads(path.read_text())
        for module in (SOURCE["PRIVATE_SOURCE_MODULE"], "example.net/alternate"):
            configured = lane.configured_mutations(path, module)
            self.assertEqual(configured["version"], original["version"])
            self.assertEqual(len(configured["mutations"]), 3)
            for before, after in zip(original["mutations"], configured["mutations"]):
                self.assertEqual(after["package"], module + "/" + before["package"])
                self.assertEqual({k: v for k, v in after.items() if k != "package"},
                                 {k: v for k, v in before.items() if k != "package"})
        self.assertEqual(json.loads(path.read_text()), original)
        with self.assertRaisesRegex(ValueError, "PRIVATE_SOURCE_MODULE"):
            lane.configured_mutations(path, "")

    # Proxy evidence follows actual published library modules, excluding similarly named unrelated modules.
    def test_retained_library_report_uses_published_modules(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proxy = root / "proxy"
            modules = []
            for name in ("board", "remoteconfig"):
                library = root / name
                library.mkdir()
                (library / "go.mod").write_text("module example.net/libraries/" + name + "\n\ngo 1.25.5\n")
                (library / "library.go").write_text("package library\n")
                modules.append(lane.publish_local_module(library, proxy))
            unrelated = proxy / "example.net/libraries/board-extra/@v/list"
            unrelated.parent.mkdir(parents=True)
            unrelated.write_text("v0.0.0\n")
            report = lane.retained_library_report(proxy, modules)
            expected = {str(p.relative_to(proxy)): lane.sha(p) for name in ("board", "remoteconfig")
                        for p in (proxy / "example.net/libraries" / name).rglob("*") if p.is_file()}
            self.assertEqual(report, expected)
            self.assertEqual(len(report), 8)


class WorkflowConfigurationTests(unittest.TestCase):
    # The real first-step shell guard rejects each absent variable before checkout and never prints values.
    def test_workflow_guard_refuses_each_missing_variable_and_accepts_values(self):
        workflow = Path(__file__).resolve().parents[3] / ".github/workflows/sh1-lanes.yml"
        text = workflow.read_text()
        marker = "      - name: Validate private source configuration\n"
        self.assertLess(text.index(marker), text.index("      - uses:"))
        block = text.split(marker, 1)[1].split("      - uses:", 1)[0]
        script = textwrap.dedent(block.split("        run: |\n", 1)[1])
        valid = subprocess.run(["bash", "-c", script], env=WORKFLOW_SOURCE, capture_output=True, text=True)
        self.assertEqual(valid.returncode, 0, valid.stderr)
        for name in WORKFLOW_SOURCE:
            for value in (None, "", " \t"):
                environment = dict(WORKFLOW_SOURCE)
                if value is None:
                    environment.pop(name)
                else:
                    environment[name] = value
                with self.subTest(name=name, value=value):
                    result = subprocess.run(["bash", "-c", script], env=environment,
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode, 1)
                    self.assertIn(name + " must be set", result.stderr)
                    for supplied in environment.values():
                        if supplied.strip():
                            self.assertNotIn(supplied, result.stdout + result.stderr)
        self.assertNotIn("vars.PRIVATE_SOURCE_", text)
        self.assertNotIn("go-version-file:", text)
        self.assertIn("go-version: ${{ needs.source.outputs.go_version }}", text)
        self.assertEqual(text.count("run: python3 host/tools/sh1/ci/checkout.py"), 2)
        for name in WORKFLOW_SOURCE:
            self.assertIn(name + ": ${{ secrets." + name + " }}", text)
        for line in text.splitlines():
            if line.lstrip().startswith("if:"):
                self.assertNotIn("secrets.", line)
