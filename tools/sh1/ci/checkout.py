"""Quiet private checkout; authentication exists only in the child process environment."""
import base64
import os
from pathlib import Path
import re
import subprocess
import sys

from run import source_configuration


class CheckoutError(RuntimeError):
    pass


def required(environment, name):
    value = environment.get(name, "")
    if not value.strip():
        raise CheckoutError(name + " must be set")
    return value


def quiet_git(arguments, environment=None, runner=subprocess.run, operation="git"):
    result = runner(["git"] + [str(arg) for arg in arguments], env=environment, capture_output=True)
    if result.returncode:
        raise CheckoutError("Private source " + operation + " failed (exit " + str(result.returncode) + ")")
    return result.stdout


def checkout(destination, repository, ref, token, sparse=None, runner=subprocess.run):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise CheckoutError("PRIVATE_SOURCE_REPO must identify an owner and repository")
    authorization = base64.b64encode(("x-access-token:" + token).encode()).decode()
    environment = dict(os.environ, GIT_TERMINAL_PROMPT="0", GIT_CONFIG_COUNT="2",
                       GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
                       GIT_CONFIG_VALUE_0="AUTHORIZATION: basic " + authorization,
                       GIT_CONFIG_KEY_1="credential.helper", GIT_CONFIG_VALUE_1="")
    quiet_git(["init", "--quiet", destination], environment, runner, "initialization")
    prefix = ["-C", destination]
    if sparse:
        quiet_git(prefix + ["sparse-checkout", "init", "--no-cone"], environment, runner, "sparse initialization")
        quiet_git(prefix + ["sparse-checkout", "set", "--no-cone", "--", sparse], environment, runner, "sparse selection")
    quiet_git(prefix + ["fetch", "--quiet", "--depth=1", "--no-tags", "--no-recurse-submodules", "--",
                        "https://github.com/" + repository + ".git", ref], environment, runner, "fetch")
    quiet_git(prefix + ["-c", "advice.detachedHead=false", "checkout", "--quiet", "--detach", "FETCH_HEAD"],
              environment, runner, "checkout")


def source_identity(destination, cli_dir):
    commit = quiet_git(["-C", destination, "rev-parse", "HEAD"], operation="identity lookup").decode().strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise CheckoutError("Private source commit identity is invalid")
    content = (destination / cli_dir / "go.mod").read_text()
    versions = re.findall(r"^go ([0-9]+\.[0-9]+(?:\.[0-9]+)?)[ \t]*(?://[^\n]*)?$", content, re.M)
    if len(versions) != 1:
        raise CheckoutError("Private source Go version directive is invalid")
    return commit, versions[0]


def main(arguments=None):
    arguments = sys.argv[1:] if arguments is None else arguments
    if arguments not in (["source"], ["lane"]):
        print("Private checkout requires source or lane mode", file=sys.stderr)
        return 1
    try:
        cli_dir, _ = source_configuration(os.environ)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    try:
        repository = required(os.environ, "PRIVATE_SOURCE_REPO")
        ref = required(os.environ, "SH1_SOURCE_REF")
        token = required(os.environ, "SPM_READ_TOKEN")
        destination = Path("board")
        checkout(destination, repository, ref, token,
                 sparse=cli_dir + "/go.mod" if arguments == ["source"] else None)
        if arguments == ["source"]:
            commit, version = source_identity(destination, cli_dir)
            with open(os.environ["GITHUB_OUTPUT"], "a") as output:
                output.write("commit=" + commit + "\ngo_version=" + version + "\n")
    except CheckoutError as error:
        print(str(error), file=sys.stderr)
        return 1
    except Exception:
        print("Private source checkout failed", file=sys.stderr)
        return 1
    print("Private source checkout ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
