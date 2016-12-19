import unittest

from zipkin_ot import util

class UtilTest(unittest.TestCase):
    
    def test_coerce_str(self):
        self.assertEqual('str', util.coerce_str('str'))
        self.assertEqual('unicode', util.coerce_str(u'unicode'))
        self.assertEqual('hard unicode char: \xe2\x80\x8b', util.coerce_str(u'hard unicode char: \u200b'))


if __name__ == '__main__':
    unittest.main()
