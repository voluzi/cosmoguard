#!/usr/bin/env python3
"""Check coordinator control flow without contacting a cluster."""
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("runner", Path(__file__).with_name("bounded-l2-cluster.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def test_idle_sampling_failure_aborts_scenario(self):
        with tempfile.TemporaryDirectory() as directory:
            run = runner.Run(SimpleNamespace(output=str(Path(directory) / "run"),
                mode="soak", duration=0, size=1024, workers=1, rps=1,
                context="unused", namespace="unused"))
            run.tools = "unused"
            driver = mock.Mock()
            driver.poll.return_value = None
            driver.wait.return_value = 0
            with mock.patch.object(run, "deploy"), mock.patch.object(run, "targets", return_value=[]), \
                 mock.patch.object(run, "scale"), mock.patch.object(run, "ttl"), \
                 mock.patch.object(run.stop, "wait", return_value=True) as wait, \
                 mock.patch.object(runner.threading, "Thread"), \
                 mock.patch.object(runner.subprocess, "Popen", return_value=driver):
                with self.assertRaisesRegex(RuntimeError, "sampling invariant failed"):
                    run.scenario(4)
                wait.assert_called_once_with(900)

    def test_successful_driver_exit_during_rollout_is_rejected(self):
        for phase in [0, 2, 5]:
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                run = runner.Run(SimpleNamespace(output=str(Path(directory) / "run"),
                    mode="soak", duration=6, size=1024, workers=1, rps=1,
                    context="unused", namespace="unused"))
                run.tools = "unused"
                driver = mock.Mock()
                driver.poll.return_value = None
                driver.wait.return_value = 0
                calls = 0
                clock = 0
                def scale(_):
                    nonlocal calls
                    if calls == phase:
                        driver.poll.return_value = 0
                    calls += 1
                def monotonic():
                    nonlocal clock
                    clock += 1
                    return clock
                with mock.patch.object(run, "deploy"), mock.patch.object(run, "targets", return_value=[]), \
                     mock.patch.object(run, "scale", side_effect=scale), mock.patch.object(run, "ttl"), \
                     mock.patch.object(run.stop, "wait", return_value=False), \
                     mock.patch.object(runner.time, "monotonic", side_effect=monotonic), \
                     mock.patch.object(runner.threading, "Thread"), \
                     mock.patch.object(runner.subprocess, "Popen", return_value=driver):
                    with self.assertRaisesRegex(RuntimeError, "traffic ended before"):
                        run.scenario(4)


if __name__ == "__main__":
    unittest.main()
