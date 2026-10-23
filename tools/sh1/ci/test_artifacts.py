"""Only the reviewed invented-fixture tooling step may publish artifacts."""
from pathlib import Path
import re
import textwrap
import unittest


WORKFLOWS = Path(__file__).resolve().parents[3] / ".github/workflows"
PUBLIC_TOOLING_UPLOAD = """\
- uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4
  if: always()
  with:
    name: sh1-${{ matrix.os }}
    path: ${{ runner.temp }}/sh1-evidence
    if-no-files-found: error"""
PRIVATE_REPORTS = ("summary.json", "manifest.json", "project.json", "inventory.json",
                   "baseline-inventory.json", "equivalence.json", "guards.json",
                   "mutations.json", "consumer.json")


def require_local_lane_reports(workflows):
    """Conservative text admission, not a general YAML parser or report redactor.

    Compare the entire upload step, so duplicate paths, added fields, wildcards,
    dynamic paths and parent directories cannot extend the reviewed allowlist.
    Unsupported upload syntax refuses instead of guessing a safe path.
    """
    for name, workflow in workflows.items():
        lines = workflow.splitlines(keepends=True)
        for index, line in enumerate(lines):
            if "actions/upload-artifact" not in line or line.lstrip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip())
            start = next((i for i in range(index, -1, -1)
                          if re.match(r"^ *- ", lines[i])
                          and len(lines[i]) - len(lines[i].lstrip()) <= indent), None)
            if start is None:
                raise AssertionError("Unreviewed artifact upload in " + name)
            indent = len(lines[start]) - len(lines[start].lstrip())
            end = next((i for i in range(start + 1, len(lines))
                        if lines[i].strip() and len(lines[i]) - len(lines[i].lstrip()) <= indent), len(lines))
            step = textwrap.dedent("".join(lines[start:end])).strip()
            if name != "sh1-tooling.yml" or step != PUBLIC_TOOLING_UPLOAD:
                raise AssertionError("Unreviewed artifact upload in " + name)


class ArtifactPublicationTests(unittest.TestCase):
    # All checked-in workflows admit only the independently reviewed tooling step.
    def test_repository_workflows_keep_lane_reports_local(self):
        workflows = {path.relative_to(WORKFLOWS).as_posix(): path.read_text() for path in WORKFLOWS.rglob("*")
                     if path.suffix in (".yml", ".yaml")}
        self.assertIn("sh1-lanes.yml", workflows)
        self.assertIn("sh1-tooling.yml", workflows)
        require_local_lane_reports(workflows)

    # Known violations fail with the policy verdict; renamed artifacts and indirect
    # directory/glob inputs cannot bypass the upload-step allowlist.
    def test_private_report_upload_plants_refuse(self):
        paths = ["${{ runner.temp }}/sh1-reports/" + name for name in PRIVATE_REPORTS]
        paths += ["summary.json", "${{ runner.temp }}/sh1-reports", "${{ runner.temp }}",
                  "**/*.json", "${{ env.REPORT_PATH }}"]
        for path in paths:
            plant = PUBLIC_TOOLING_UPLOAD.replace("${{ runner.temp }}/sh1-evidence", path)
            with self.subTest(path=path), self.assertRaisesRegex(AssertionError, "Unreviewed artifact upload"):
                require_local_lane_reports({"sh1-tooling.yml": plant})
        # Even the public step is not approved inside a workflow with private inputs.
        with self.assertRaisesRegex(AssertionError, "Unreviewed artifact upload"):
            require_local_lane_reports({"sh1-lanes.yml": PUBLIC_TOOLING_UPLOAD})

    # The actual public invented-fixture upload is a positive control, not a mock path.
    def test_actual_tooling_upload_is_admitted(self):
        text = (WORKFLOWS / "sh1-tooling.yml").read_text()
        self.assertIn(textwrap.indent(PUBLIC_TOOLING_UPLOAD, "      "), text)
        require_local_lane_reports({"sh1-tooling.yml": text})

    # Additional path keys and alternate upload layouts fail closed; the literal
    # allowlist is bounded to reviewed workflow text, not arbitrary YAML encodings.
    def test_extended_or_unsupported_upload_steps_refuse(self):
        for plant in (PUBLIC_TOOLING_UPLOAD + "\n    path: summary.json",
                      PUBLIC_TOOLING_UPLOAD.replace("path: ${{ runner.temp }}/sh1-evidence",
                                                    "path: |\n      ${{ runner.temp }}/sh1-evidence\n      summary.json"),
                      "- {uses: actions/upload-artifact@v4, with: {path: summary.json}}"):
            with self.subTest(plant=plant), self.assertRaisesRegex(AssertionError, "Unreviewed artifact upload"):
                require_local_lane_reports({"sh1-tooling.yml": plant})
