import unittest


from ParseLog import parse_log
import datetime

class MyTestCase(unittest.TestCase):
    def test_parser(self):
        self.assertEqual(parse_log(
            "176.181.12.220 - - [17/Mar/2024:06:46:44 +0100] \"GET /structures/fiche/HTTP/1.1\" 302 1904 \"-\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36\" 0"
        ),
        ('176.181.12.220',
         datetime.datetime(2024, 3, 17, 6, 46, 44, tzinfo=datetime.timezone(datetime.timedelta(seconds=3600))),
         'GET /structures/fiche/ HTTP/1.1',
         302,
         1904,
         "Empty",
         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36')
        )  # add assertion here

    def test_timestamp_when_fail(self):
        self.assertEqual(parse_log(
         "193.32.126.228 - - [23/Jun/2025:02:24:22 +0200] \"GET /personnes/109.70.100.2 - - [23/Jun/2025:02:25:23 +0200] \"GET / HTTP/1.1\" 200 6776 \"-\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0\" 0"),
    ('176.181.12.220',
         datetime.datetime(2025, 3, 17, 6, 46, 44, tzinfo=datetime.timezone(datetime.timedelta(seconds=3600))),
         'GET /structures/fiche/ HTTP/1.1',
         302,
         1904,
         "Empty",
         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36')
        )


if __name__ == '__main__':
    unittest.main()
