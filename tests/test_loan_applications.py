"""Unit tests for loan application reference prefix helper (no DB)."""

from __future__ import annotations

import unittest

from loan_management.loan_applications import agent_surname_prefix


class TestAgentSurnamePrefix(unittest.TestCase):
    def test_non_agent_prefix(self):
        self.assertEqual(agent_surname_prefix(None), "NON")
        self.assertEqual(agent_surname_prefix(""), "NON")

    def test_single_name_uses_token(self):
        self.assertEqual(agent_surname_prefix("Mupaya"), "MUP")

    def test_multi_word_uses_last_token(self):
        self.assertEqual(agent_surname_prefix("John Mupaya"), "MUP")

    def test_pads_short_name(self):
        self.assertEqual(len(agent_surname_prefix("Al", width=3)), 3)

    def test_strips_non_letters(self):
        self.assertEqual(agent_surname_prefix("O'Brien"), "OBR")


if __name__ == "__main__":
    unittest.main()
