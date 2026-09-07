import unittest

from check_models import evidence


COMPLETE = """Running with fp 42 and seed 1234
Model checking completed. No error has been found.
1,255 states generated, 315 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 34.
"""


class EvidenceTests(unittest.TestCase):
    def test_complete_evidence(self):
        result = evidence(COMPLETE, 0)
        self.assertEqual(result["status"], "passed")
        self.assertEqual((result["generated"], result["distinct"], result["depth"]), (1255, 315, 34))
        self.assertEqual((result["fingerprint"], result["seed"]), (42, 1234))

    def test_partial_or_missing_evidence_is_not_success(self):
        for log in ("", COMPLETE.replace("No error has been found.", "Interrupted."),
                    COMPLETE.replace("0 states left", "10 states left"),
                    COMPLETE.replace("315 distinct", "0 distinct"),
                    COMPLETE.replace("with fp 42 and seed 1234", "seed unavailable"),
                    COMPLETE.split("The depth")[0]):
            with self.subTest(log=log):
                self.assertEqual(evidence(log, 0)["status"], "incomplete")

    def test_failures_override_success_markers(self):
        self.assertEqual(evidence(COMPLETE, 1)["status"], "failed")
        self.assertEqual(evidence(COMPLETE + "Error: Invariant X is violated.", 0)["status"], "failed")
        self.assertEqual(evidence(COMPLETE, -9, timed_out=True)["status"], "timeout")


if __name__ == "__main__":
    unittest.main()
