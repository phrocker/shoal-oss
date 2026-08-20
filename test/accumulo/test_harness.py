import importlib.util
from pathlib import Path
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).with_name("harness.py")
SPEC = importlib.util.spec_from_file_location("accumulo_harness", MODULE_PATH)
assert SPEC and SPEC.loader
harness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(harness)


class HarnessTest(unittest.TestCase):
    def test_static_configuration_matches_exact_target(self):
        self.assertEqual([], harness.validate())
        self.assertEqual("4.0.0-SNAPSHOT", harness.ACCUMULO_VERSION)
        self.assertEqual(
            "1a716b2c1bb5762ead4b46d2bc4f53e13873b314",
            harness.ACCUMULO_REVISION,
        )

    def test_compose_command_is_stable_and_project_scoped(self):
        command = harness.compose_command("up", "--detach", "--build")
        self.assertEqual("docker", command[0])
        self.assertEqual(
            [
                "compose",
                "--project-name",
                "shoal-accumulo4-test",
                "--file",
                str(harness.COMPOSE_FILE),
                "up",
                "--detach",
                "--build",
            ],
            command[1:],
        )

    def test_java_client_command_is_noninteractive(self):
        self.assertEqual(
            ["exec", "-T", "manager", "bash", "/opt/shoal-smoke/run.sh", "smoke"],
            harness.client_command("smoke")[-6:],
        )

    @mock.patch.object(harness, "down")
    @mock.patch.object(harness, "wait_ready", side_effect=RuntimeError("not ready"))
    @mock.patch.object(harness, "run")
    def test_failed_start_logs_and_cleans_up(self, run, _wait_ready, down):
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            harness.start_and_wait()
        self.assertEqual(2, down.call_count)
        self.assertEqual(
            harness.compose_command("logs", "--no-color"),
            run.call_args_list[-1].args[0],
        )

    @mock.patch.object(harness.shutil, "which", return_value=None)
    def test_missing_docker_is_an_explicit_skip_error(self, _which):
        with self.assertRaisesRegex(
            harness.DockerUnavailable, "Docker CLI is not installed"
        ):
            harness.require_docker()


if __name__ == "__main__":
    unittest.main()
