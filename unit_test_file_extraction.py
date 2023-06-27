import unittest
from file_extraction import *


class UnitTests(unittest.TestCase):

    def setUp(self) -> None:
        self.s3 = boto3.client('s3')
        self.files_dict = {'academy_csv': [], 'json': [], 'txt': [], 'csv': []}

    def test_extract_academy_csv(self):
        extract_file_type(self.s3, 'Academy', self.files_dict)
        actual = len(self.files_dict['academy_csv'])
        expected = 36
        self.assertEqual(
            actual, expected,
            "Expected `extract_file_type` method to create a list of 36 file objects."
        )

    def test_extract_talent_csv(self):
        extract_file_type(self.s3, 'Talent', self.files_dict, 'csv')
        actual = len(self.files_dict['csv'])
        expected = 12
        self.assertEqual(
            actual, expected,
            "Expected `extract_file_type` method to create a list of 12 file objects."
        )

    def test_extract_talent_txt(self):
        extract_file_type(self.s3, 'Talent', self.files_dict, 'txt')
        actual = len(self.files_dict['txt'])
        expected = 152
        self.assertEqual(
            actual, expected,
            "Expected `extract_file_type` method to create a list of 152 file objects."
        )

    # def test_extract_talent_json(self):
    #     extract_file_type(self.s3, 'Talent', self.files_dict, 'json')
    #     actual = len(self.files_dict['json'])
    #     expected = 999
    #     self.assertEqual(
    #         actual, expected,
    #         "Expected `extract_file_type` method to create a list of 999+ file objects."
    #     )


if __name__ == "__main__":
    unittest.main()
