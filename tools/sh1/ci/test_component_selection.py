"""Static witness for the public component suite's command selection."""

from pathlib import Path
import re
import shlex
import unittest


class ComponentSelectionTests(unittest.TestCase):
    # Checks command selectors and explicit GOFLAGS declarations, not execution,
    # arbitrary scripts, conditional steps or platform health.
    def workflow_source(self):
        workflow = Path(__file__).resolve().parents[3] / ".github/workflows/sh1-lanes.yml"
        return workflow.read_text()

    def assert_full_component_selector(self, source):
        # Scan explicit keys at every env scope. Only the known literal is
        # admitted; expressions, aliases and multiline/other values are unknown.
        for declaration in re.finditer(
            r'''(?m)(?<![\w])(?:GOFLAGS|'GOFLAGS'|"GOFLAGS")[ \t]*:[ \t]*(.*)$''', source
        ):
            self.assertRegex(
                declaration.group(1),
                r'''^(?:-mod=readonly|'-mod=readonly'|"-mod=readonly")(?:[ \t]+\#.*)?[ \t]*$''',
                "GOFLAGS must be literal -mod=readonly without test selection or unknown values",
            )
            # A deeper next content line can continue a YAML plain scalar.
            # Ignore blank/comment lines, but never admit a continuation value.
            prefix = source[:declaration.start()].rsplit("\n", 1)[-1]
            indent = len(prefix) - len(prefix.lstrip(" \t"))
            for following in source[declaration.end():].splitlines():
                if not following.strip() or following.lstrip().startswith("#"):
                    continue
                self.assertLessEqual(
                    len(following) - len(following.lstrip(" \t")),
                    indent,
                    "GOFLAGS must be literal -mod=readonly without continuation lines",
                )
                break
        commands = []
        for line in source.splitlines():
            command = line.strip().removeprefix("run: ")
            if command.startswith("go test "):
                commands.append(shlex.split(command))
        self.assertEqual(len(commands), 1, "expected one full component test command")
        command = commands[0]
        self.assertEqual(command[:2], ["go", "test"])
        selectors = [arg for arg in command[2:] if not arg.startswith("-")]
        self.assertEqual(
            selectors,
            ["./..."],
            "whole module selector must include current and future host packages",
        )
        self.assertEqual(
            [arg for arg in command[2:] if arg.startswith("-")],
            ["-p=1", "-parallel=1", "-count=1"],
            "component suites must run uncached without test-name, skip or tag filters",
        )

    def test_full_component_selector_covers_every_package(self):
        self.assert_full_component_selector(self.workflow_source())

    # Proves an old component list or an explicit list missing a newly composed
    # service cannot satisfy the whole-module command. This checks selection,
    # including future cmd packages; it does not execute any Go test.
    def test_whole_module_selector_refuses_partial_component_lists(self):
        source = self.workflow_source()
        self.assert_full_component_selector(source)
        old = ["sessionmanager", "musepty", "sessionplugin", "sessionclient", "musebuilds"]
        current = old + ["hostdefaults", "sessiongoal", "sessioncontext", "sessionbudget"]
        for components in [old] + [current[:i] + current[i + 1:] for i in range(len(current))]:
            with self.subTest(components=components):
                replacement = " ".join(f"./{component}/..." for component in components)
                fixture, count = re.subn(
                    r"(?m)^(\s*go test -p=1 -parallel=1 -count=1) \.\/\.\.\.$",
                    lambda match: match.group(1) + " " + replacement,
                    source,
                )
                self.assertEqual(count, 1, "partial selector fixture must replace the actual test command")
                with self.assertRaisesRegex(AssertionError, "whole module selector"):
                    self.assert_full_component_selector(fixture)

    def goflags_fixture(self, source, scope, value):
        if scope == "workflow":
            fixture = f"env:\n  GOFLAGS: {value}\n" + source
            self.assertTrue(fixture.startswith(f"env:\n  GOFLAGS: {value}\n"))
            return fixture
        if scope == "job":
            declaration = f"      GOFLAGS: {value}\n"
            fixture, count = re.subn(
                r"(?m)^      GOFLAGS:[^\n]*\n", lambda _: declaration, source
            )
        else:
            self.assertEqual(scope, "step")
            declaration = f"        env:\n          GOFLAGS: {value}\n"
            fixture, count = re.subn(
                r"(?m)^      - name: Full component suites\n",
                lambda match: match.group() + declaration,
                source,
            )
        self.assertEqual(count, 1, f"{scope} GOFLAGS fixture must replace exactly one anchor")
        self.assertEqual(fixture.count(declaration), 1, f"{scope} GOFLAGS fixture not materialized")
        return fixture

    # Proves explicit workflow/job/step GOFLAGS cannot reduce selection, with a
    # literal readonly control at each scope. It does not evaluate shell scripts.
    def test_goflags_declarations_cannot_filter_component_suites(self):
        source = self.workflow_source()

        for scope in ("workflow", "job", "step"):
            with self.subTest(scope=scope, value="literal readonly positive"):
                self.assert_full_component_selector(self.goflags_fixture(source, scope, '"-mod=readonly"'))
            for value in (
                "-mod=readonly -run=^TestNoSuchComponentTest$",
                "-mod=readonly -skip=.",
                "-mod=readonly -tags=mutants",
                "-mod=readonly -short",
                "-mod=readonly -list=.",
                "${{ inputs.goflags }}",
            ):
                with self.subTest(scope=scope, value=value):
                    with self.assertRaisesRegex(AssertionError, "GOFLAGS must be literal"):
                        self.assert_full_component_selector(self.goflags_fixture(source, scope, repr(value)))

    # Proves deeper plain-scalar continuations are refused at all three actual
    # workflow scopes, even across blank/comment lines; nearby single-line
    # readonly spellings remain valid. This is a bounded scan, not YAML parsing.
    def test_multiline_goflags_cannot_filter_component_suites(self):
        source = self.workflow_source()
        for scope, indent in (("workflow", 2), ("job", 6), ("step", 10)):
            for value in ("-mod=readonly", "'-mod=readonly'", '\"-mod=readonly\"'):
                with self.subTest(scope=scope, value=value, control="single line"):
                    self.assert_full_component_selector(self.goflags_fixture(source, scope, value))
                with self.subTest(scope=scope, value=value, control="comment and blank"):
                    self.assert_full_component_selector(self.goflags_fixture(
                        source, scope, value + " # readonly\n\n" + " " * (indent + 2) + "# comment"
                    ))
            for gap in ("", "\n", " " * (indent + 2) + "# comment\n", "\n# comment\n"):
                value = "-mod=readonly\n" + gap + " " * (indent + 2) + "-run=^TestNoSuchComponentTest$"
                with self.subTest(scope=scope, gap=gap):
                    fixture = self.goflags_fixture(source, scope, value)
                    with self.assertRaisesRegex(AssertionError, "GOFLAGS must be literal.*continuation"):
                        self.assert_full_component_selector(fixture)


if __name__ == "__main__":
    unittest.main()
