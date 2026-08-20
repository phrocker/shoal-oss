#!/usr/bin/env python3
"""Deterministic local Accumulo 4 conformance harness lifecycle."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "test" / "accumulo"
COMPOSE_FILE = HARNESS / "docker-compose.yml"
PROJECT = "shoal-accumulo4-test"
ACCUMULO_REVISION = "1a716b2c1bb5762ead4b46d2bc4f53e13873b314"
ACCUMULO_VERSION = "4.0.0-SNAPSHOT"
ACCUMULO_SOURCE_SHA512 = (
    "bf2cac6f7b5f759358ac306bba2342dc74c51bf63d70905da1004fc1eb0cfa66"
    "f6a73023ab250824a67f17d410e9b3351f097ddfd742bbc4b14f7a7feb6c4b2e"
)
HADOOP_VERSION = "3.4.2"
HADOOP_SHA512 = (
    "79a383e156022d6690da359120b25db8146452265d92a4e890d9ea78c2078a01"
    "b661daf78163ee9b4acef7106b01fd5c8d1a55f7ad284f88b31ab3f402ae3acf"
)
ZOOKEEPER_VERSION = "3.9.5"


class DockerUnavailable(RuntimeError):
    pass


def compose_command(*args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-name",
        PROJECT,
        "--file",
        str(COMPOSE_FILE),
        *args,
    ]


def client_command(mode: str) -> list[str]:
    return compose_command(
        "exec", "-T", "manager", "bash", "/opt/shoal-smoke/run.sh", mode
    )


def _properties(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or not value:
            raise ValueError(f"{path}: invalid property line {raw!r}")
        if key in result:
            raise ValueError(f"{path}: duplicate property {key}")
        result[key] = value
    return result


def validate(root: Path = ROOT) -> list[str]:
    harness = root / "test" / "accumulo"
    errors: list[str] = []

    compose = (harness / "docker-compose.yml").read_text(encoding="utf-8")
    dockerfile = (harness / "image" / "Dockerfile").read_text(encoding="utf-8")
    required_text = {
        "compose Accumulo image": (
            compose,
            "shoal/accumulo:4.0.0-snapshot-1a716b2c",
        ),
        "compose Hadoop image": (compose, f"apache/hadoop:{HADOOP_VERSION}"),
        "compose ZooKeeper image": (compose, f"zookeeper:{ZOOKEEPER_VERSION}"),
        "Dockerfile Accumulo revision": (
            dockerfile,
            f"ARG ACCUMULO_REVISION={ACCUMULO_REVISION}",
        ),
        "Dockerfile Accumulo checksum": (
            dockerfile,
            f"ARG ACCUMULO_SOURCE_SHA512={ACCUMULO_SOURCE_SHA512}",
        ),
        "Dockerfile Hadoop version": (
            dockerfile,
            f"ARG HADOOP_VERSION={HADOOP_VERSION}",
        ),
        "Dockerfile Hadoop checksum": (
            dockerfile,
            f"ARG HADOOP_SHA512={HADOOP_SHA512}",
        ),
        "Java smoke mount": (compose, "./smoke:/opt/shoal-smoke:ro"),
    }
    for description, (text, expected) in required_text.items():
        if expected not in text:
            errors.append(f"{description} is not pinned to {expected!r}")

    if "2.1.6" in compose or "2.1.6" in dockerfile:
        errors.append("legacy Accumulo 2.1.6 reference remains")
    if "SHOAL_ACCUMULO_IMAGE" in compose:
        errors.append("exact-target Accumulo image must not be environment-overridable")

    try:
        client = _properties(harness / "conf" / "accumulo-client.properties")
        expected_client = {
            "instance.name": "shoal",
            "instance.zookeepers": "zookeeper:2181",
            "auth.type": "password",
            "auth.principal": "root",
            "auth.token": "secret",
        }
        if client != expected_client:
            errors.append("accumulo-client.properties differs from the harness contract")
    except (OSError, ValueError) as exc:
        errors.append(str(exc))

    try:
        accumulo = _properties(harness / "conf" / "accumulo.properties")
        expected_accumulo = {
            "instance.volumes": "hdfs://namenode:8020/accumulo",
            "instance.zookeeper.host": "zookeeper:2181",
            "instance.secret": "shoal-test-instance-secret",
            "tserver.memory.maps.native.enabled": "false",
            "table.durability": "flush",
        }
        if accumulo != expected_accumulo:
            errors.append("accumulo.properties differs from the exact-target contract")
    except (OSError, ValueError) as exc:
        errors.append(str(exc))

    for name in ("core-site.xml", "hdfs-site.xml", "hdfs-client-site.xml"):
        try:
            ET.parse(harness / "conf" / name)
        except (OSError, ET.ParseError) as exc:
            errors.append(f"{name}: {exc}")

    return errors


def require_docker() -> None:
    if shutil.which("docker") is None:
        raise DockerUnavailable("Docker CLI is not installed or not on PATH")
    result = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        detail = result.stderr.strip() or "Docker daemon did not report a version"
        raise DockerUnavailable(detail)
    compose = subprocess.run(
        ["docker", "compose", "version"],
        text=True,
        capture_output=True,
        check=False,
    )
    if compose.returncode != 0:
        raise DockerUnavailable(
            compose.stderr.strip() or "Docker Compose v2 is unavailable"
        )


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", subprocess.list2cmdline(command), flush=True)
    return subprocess.run(command, text=True, check=check)


def wait_ready() -> None:
    timeout = int(os.environ.get("SHOAL_ACCUMULO_READY_TIMEOUT", "300"))
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        result = subprocess.run(
            client_command("ready"),
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            print(f"Accumulo {ACCUMULO_VERSION} ready after {attempt} probe(s)")
            return
        time.sleep(2)
    raise RuntimeError(f"Accumulo did not become ready within {timeout} seconds")


def down() -> None:
    run(compose_command("down", "--volumes", "--remove-orphans"), check=False)


def start_and_wait() -> None:
    down()
    try:
        run(compose_command("up", "--detach", "--build"))
        wait_ready()
    except BaseException:
        run(compose_command("logs", "--no-color"), check=False)
        down()
        raise


def full_run() -> None:
    require_docker()
    down()
    start_attempted = False
    succeeded = False
    previous_handlers: dict[int, object] = {}

    def terminate(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, terminate)
    try:
        start_attempted = True
        run(compose_command("up", "--detach", "--build"))
        wait_ready()
        run(client_command("smoke"))
        succeeded = True
        print(f"Accumulo {ACCUMULO_VERSION} Java create/write/flush/scan smoke passed")
    finally:
        if start_attempted and not succeeded:
            run(compose_command("logs", "--no-color"), check=False)
        down()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=("validate", "up", "wait", "smoke", "down", "test")
    )
    args = parser.parse_args()

    errors = validate()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    if args.action == "validate":
        print(
            f"Accumulo {ACCUMULO_VERSION} harness configuration is valid "
            f"(revision {ACCUMULO_REVISION})"
        )
        return 0

    try:
        require_docker()
        if args.action == "up":
            start_and_wait()
        elif args.action == "wait":
            wait_ready()
        elif args.action == "smoke":
            run(client_command("smoke"))
        elif args.action == "down":
            down()
        else:
            full_run()
    except DockerUnavailable as exc:
        print(
            f"SKIP (needs-docker): {exc}; live Accumulo 4 test was not run",
            file=sys.stderr,
        )
        return 2
    except (RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
