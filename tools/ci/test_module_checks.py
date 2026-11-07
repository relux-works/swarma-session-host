import contextlib
import io
import json
import unittest

import module_checks as checks


class AdmissionTests(unittest.TestCase):
    def admission(self, loads):
        samples = iter(loads)
        now = [0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        with contextlib.redirect_stdout(io.StringIO()):
            admitted = checks.admit(lambda: (next(samples), 0, 0), lambda: now[0], sleep)
        return admitted, now[0], sleeps

    def test_immediate_low_load(self):
        self.assertEqual(self.admission([19.99]), (True, 0, []))

    def test_waits_for_load_to_drop(self):
        self.assertEqual(self.admission([109.3, 24.9, 19]), (True, 10, [5, 5]))

    def test_exact_threshold_refuses_with_bounded_wait(self):
        admitted, elapsed, sleeps = self.admission([20] * 25)
        self.assertFalse(admitted)
        self.assertEqual(elapsed, 120)
        self.assertEqual(sleeps, [5] * 24)

    def test_probe_error_refuses(self):
        def broken_probe():
            raise OSError("load unavailable")

        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(checks.admit(broken_probe))


class CheckResultsTests(unittest.TestCase):
    def run_checks(self, admissions, exits):
        admissions, exits = iter(admissions), iter(exits)
        called = []

        def run(command):
            called.append(command)
            return next(exits)

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = checks.run_checks(lambda: next(admissions), run)
        records = [json.loads(line) for line in output.getvalue().splitlines()
                   if line.startswith("{")]
        return code, called, records

    def test_only_actual_passes_are_green(self):
        code, called, records = self.run_checks([True, True], [0, 0])
        self.assertEqual(code, 0)
        self.assertEqual(called, [command for _, command in checks.CHECKS])
        self.assertEqual([r["status"] for r in records[:-1]], ["pass", "pass"])

    def test_refused_vet_does_not_suppress_race_or_invent_vet_exit(self):
        code, called, records = self.run_checks([False, True], [0])
        self.assertEqual(code, 125)
        self.assertEqual(called, [checks.CHECKS[1][1]])
        self.assertEqual(records[0]["status"], "unavailable")
        self.assertIsNone(records[0]["exit_code"])
        self.assertEqual(records[0]["admission_exit_code"], 125)
        self.assertEqual(records[-1]["overall"], "unavailable")

    def test_refused_race_and_both_refused_are_not_green(self):
        for admissions, exits, statuses in (([True, False], [0], ["pass", "unavailable"]),
                                           ([False, False], [], ["unavailable", "unavailable"])):
            with self.subTest(admissions=admissions):
                code, _, records = self.run_checks(admissions, exits)
                self.assertEqual(code, 125)
                self.assertEqual([r["status"] for r in records[:-1]], statuses)

    def test_actual_failure_preserved_and_other_check_still_attempted(self):
        for admissions, exits, expected_calls in (([True, True], [7, 0], 2),
                                                   ([True, False], [7], 1),
                                                   ([False, True], [9], 1),
                                                   ([True, True], [0, 125], 2)):
            with self.subTest(admissions=admissions, exits=exits):
                code, called, records = self.run_checks(admissions, exits)
                self.assertEqual(code, 1)
                self.assertEqual(len(called), expected_calls)
                self.assertEqual([r["exit_code"] for r in records[:-1]
                                  if r["exit_code"] is not None], exits)
                self.assertEqual(records[-1]["overall"], "fail")

    def test_missing_tool_is_not_a_product_exit(self):
        calls = []

        def missing(command):
            calls.append(command)
            raise FileNotFoundError("tool unavailable")

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(checks.run_checks(lambda: True, missing), 125)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(r["exit_code"] is None for r in records[:-1]))
        self.assertTrue(all(r["reason"] == "check_start_failed" for r in records[:-1]))


if __name__ == "__main__":
    unittest.main()
