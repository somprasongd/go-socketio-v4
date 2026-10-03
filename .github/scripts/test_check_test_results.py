import unittest

from check_test_results import REQUIRED, check_events


def acceptance_passes():
    return [{"Action": "pass", "Package": package, "Test": test} for package, test in REQUIRED]


class AcceptanceGateTest(unittest.TestCase):
    def test_successful_acceptance(self):
        self.assertEqual(check_events(acceptance_passes()), [])

    def test_unrelated_unit_failure_is_not_hidden_by_acceptance_passes(self):
        events = acceptance_passes() + [{"Action": "fail", "Package": "parser", "Test": "TestRegression"}]
        self.assertIn("TestRegression", "\n".join(check_events(events)))

    def test_package_failure_without_a_named_test_blocks_release(self):
        events = acceptance_passes() + [{"Action": "fail", "Package": "engineio"}]
        self.assertIn("engineio", "\n".join(check_events(events)))

    def test_skipped_acceptance_is_not_a_pass(self):
        package, test = next(iter(REQUIRED))
        events = [event for event in acceptance_passes() if event["Test"] != test]
        events.append({"Action": "skip", "Package": package, "Test": test})
        self.assertIn(test, "\n".join(check_events(events)))


if __name__ == "__main__":
    unittest.main()
