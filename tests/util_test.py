import unittest

from zipkin_ot import util

class UtilTest(unittest.TestCase):

    def test_coerce_str(self):
        self.assertEqual(b'str', util.coerce_str(b'str'))
        self.assertEqual(b'unicode', util.coerce_str(u'unicode'))
        self.assertEqual(b'hard unicode char: \xe2\x80\x8b', util.coerce_str(u'hard unicode char: \u200b'))


if __name__ == '__main__':
    unittest.main()
