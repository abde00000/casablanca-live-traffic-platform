import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from collector.collector import BudgetExceeded, MonthlyRequestBudget


class MonthlyRequestBudgetTests(unittest.TestCase):
    def test_budget_stops_before_limit_is_exceeded(self):
        with tempfile.TemporaryDirectory() as tmp:
            budget = MonthlyRequestBudget(Path(tmp) / "budget.json", monthly_limit=2)
            now = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
            self.assertEqual(budget.reserve(now), 1)
            self.assertEqual(budget.reserve(now), 2)
            with self.assertRaises(BudgetExceeded):
                budget.reserve(now)

    def test_budget_resets_for_new_utc_month(self):
        with tempfile.TemporaryDirectory() as tmp:
            budget = MonthlyRequestBudget(Path(tmp) / "budget.json", monthly_limit=2)
            october = datetime(2026, 10, 31, 23, 59, tzinfo=timezone.utc)
            november = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
            self.assertEqual(budget.reserve(october), 1)
            self.assertEqual(budget.reserve(november), 1)


if __name__ == "__main__":
    unittest.main()
