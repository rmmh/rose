import json
import unittest

from check_go import test_evidence


def events(*items):
    return [json.dumps(dict(Package="example", **item)) for item in items]


class EvidenceTests(unittest.TestCase):
    def test_skip_is_visible(self):
        result = test_evidence(events(
            dict(Action="start"), dict(Action="run", Test="TestGood"),
            dict(Action="pass", Test="TestGood"),
            dict(Action="skip", Test="TestMount"), dict(Action="pass")), 0)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["coverage"], "partial")
        self.assertEqual(result["skipped_tests"], ["example/TestMount"])

    def test_empty_partial_and_all_skipped_cannot_pass(self):
        for log in ([], ["broken json"], events(dict(Action="start")),
                    events(dict(Action="pass", Test="TestGood")),
                    events(dict(Action="run", Test="TestGood"), dict(Action="pass")),
                    events(dict(Action="skip", Test="TestMount"), dict(Action="pass"))):
            with self.subTest(log=log):
                self.assertEqual(test_evidence(log, 0)["status"], "incomplete")

    def test_failures_override_success(self):
        good = events(dict(Action="pass", Test="TestGood"), dict(Action="pass"))
        self.assertEqual(test_evidence(good, 0)["status"], "passed")
        self.assertEqual(test_evidence(good, 1)["status"], "failed")
        for extra in (dict(Action="fail"), dict(Action="fail", Test="TestBad")):
            self.assertEqual(test_evidence(good + events(extra), 0)["status"], "failed")

    def test_required_coverage_rejects_skips(self):
        good = events(dict(Action="pass", Test="TestGood"), dict(Action="pass"))
        self.assertEqual(test_evidence(good, 0, require_no_skips=True)["status"], "passed")
        for skipped in (dict(Action="skip", Test="TestMount"), dict(Action="skip")):
            with self.subTest(skipped=skipped):
                self.assertEqual(test_evidence(good + events(skipped), 0,
                                               require_no_skips=True)["status"], "failed")


if __name__ == "__main__":
    unittest.main()
