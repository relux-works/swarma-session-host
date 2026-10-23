"""Private checkout tests use a local public fixture and invented credentials."""
import base64
import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import checkout


class QuietCheckoutTests(unittest.TestCase):
    # Real Git init/fetch/detach/sparse operations stay quiet; only their network URL is mapped to this local repo.
    def test_local_git_checkout_preserves_pinned_bytes_without_persisting_authentication(self):
        origin = Path(__file__).resolve().parents[3]
        commit = subprocess.run(["git", "-C", str(origin), "rev-parse", "HEAD"],
                                check=True, capture_output=True, text=True).stdout.strip()
        for sparse in (None, "testdata/sh1-fixture/go.mod"):
            with self.subTest(sparse=sparse), tempfile.TemporaryDirectory() as directory:
                destination = Path(directory) / "board"
                calls = []

                def local_fetch(arguments, **kwargs):
                    calls.append((list(arguments), dict(kwargs["env"])))
                    arguments = list(arguments)
                    if "fetch" in arguments:
                        arguments[arguments.index("--") + 1] = str(origin)
                    return subprocess.run(arguments, **kwargs)

                stdout, stderr = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    checkout.checkout(destination, "example/source", commit, "invented-token", sparse, local_fetch)
                    actual, version = checkout.source_identity(destination, "testdata/sh1-fixture")
                self.assertEqual((actual, version), (commit, "1.25.5"))
                self.assertEqual(stdout.getvalue() + stderr.getvalue(), "")
                config = (destination / ".git/config").read_text()
                self.assertNotIn("extraheader", config)
                self.assertNotIn("invented-token", config)
                encoded = base64.b64encode(b"x-access-token:invented-token").decode()
                self.assertNotIn(encoded, config)
                for arguments, environment in calls:
                    self.assertNotIn("invented-token", " ".join(arguments))
                    self.assertNotIn(encoded, " ".join(arguments))
                    self.assertEqual(environment["GIT_CONFIG_VALUE_0"], "AUTHORIZATION: basic " + encoded)
                expected = subprocess.run(["git", "-C", str(origin), "show", commit + ":testdata/sh1-fixture/go.mod"],
                                          check=True, capture_output=True).stdout
                self.assertEqual((destination / "testdata/sh1-fixture/go.mod").read_bytes(), expected)
                if sparse:
                    self.assertFalse((destination / "tools/sh1/main.go").exists())
                else:
                    self.assertTrue((destination / "tools/sh1/main.go").is_file())

    # Failing Git can emit arbitrary derived identities/auth data, but none is forwarded to logs or exceptions.
    def test_failed_git_diagnostics_are_not_echoed(self):
        payload = b"EXAMPLE/SOURCE example%2Fsource invented-token"

        def failure(arguments, **kwargs):
            return subprocess.CompletedProcess(arguments, 128, payload, payload)

        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with self.assertRaisesRegex(checkout.CheckoutError, r"^Private source initialization failed \(exit 128\)$"):
                checkout.checkout(Path("unused"), "example/source", "main", "invented-token", runner=failure)
        self.assertEqual(stdout.getvalue() + stderr.getvalue(), "")

    # Only a numeric Go directive may become a public setup-go input; injected/multiple directives refuse.
    def test_source_version_rejects_non_numeric_and_duplicate_directives(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cli = root / "cli"
            cli.mkdir()
            with patch.object(checkout, "quiet_git", return_value=b"a" * 40):
                for directive in ("go example.org/private", "go 1.25.5\ngo 1.26.0", "// no version"):
                    (cli / "go.mod").write_text("module example.org/fixture\n" + directive + "\n")
                    with self.assertRaisesRegex(checkout.CheckoutError, "Go version directive is invalid"):
                        checkout.source_identity(root, "cli")
                (cli / "go.mod").write_text("module example.org/fixture\ngo 1.25.5 // supported comment\n")
                self.assertEqual(checkout.source_identity(root, "cli"), ("a" * 40, "1.25.5"))

    # Missing auth/settings refuse before any checkout; unexpected errors do not echo sensitive exception text.
    def test_entry_refuses_missing_configuration_and_hides_unexpected_errors(self):
        environment = {"PRIVATE_SOURCE_REPO": "example/source", "PRIVATE_SOURCE_CLI_DIR": "tools/source-cli",
                       "PRIVATE_SOURCE_MODULE": "example.org/source-cli", "SH1_SOURCE_REF": "main",
                       "SPM_READ_TOKEN": "invented-token"}
        for name in environment:
            invalid = dict(environment)
            invalid.pop(name)
            error = io.StringIO()
            with patch.dict(os.environ, invalid, clear=True), patch.object(checkout, "checkout") as fetch, contextlib.redirect_stderr(error):
                self.assertEqual(checkout.main(["lane"]), 1)
                fetch.assert_not_called()
                self.assertIn(name + " must be set", error.getvalue())
        with patch.dict(os.environ, environment, clear=True), \
                patch.object(checkout, "checkout", side_effect=OSError("EXAMPLE%2FSOURCE")), \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(checkout.main(["lane"]), 1)
        self.assertEqual(error.getvalue(), "Private source checkout failed\n")
