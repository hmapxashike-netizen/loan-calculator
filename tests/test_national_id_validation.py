"""National ID format helper tests."""

from __future__ import annotations

import unittest

from customers.national_id_validation import (
    is_valid_national_id_format,
    normalize_national_id_input,
)


class TestNationalIdValidation(unittest.TestCase):
    def test_valid_eight_digits(self):
        self.assertTrue(is_valid_national_id_format("12345678A12"))
        self.assertTrue(is_valid_national_id_format("12345678a12"))

    def test_valid_seven_digits(self):
        self.assertTrue(is_valid_national_id_format("1234567A12"))
        self.assertEqual(normalize_national_id_input("1234567a12"), "1234567A12")

    def test_valid_nine_digits(self):
        self.assertTrue(is_valid_national_id_format("123456789A12"))

    def test_normalize_check_letter_upper(self):
        self.assertEqual(normalize_national_id_input("12345678a12"), "12345678A12")

    def test_rejects_too_few_digits_before_letter(self):
        self.assertFalse(is_valid_national_id_format("123456A12"))

    def test_rejects_empty(self):
        self.assertFalse(is_valid_national_id_format(""))

    def test_rejects_no_letter(self):
        self.assertFalse(is_valid_national_id_format("12345678912"))

    def test_rejects_too_many_digits_before_letter(self):
        # 10 digits before check letter (max is 9)
        self.assertFalse(is_valid_national_id_format("1234567890A12"))


if __name__ == "__main__":
    unittest.main()
