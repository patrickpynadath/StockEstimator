from unittest import TestCase
import datetime as dt
from .frequency_chunker import FreqChunker

class TestFreqChunker(TestCase):
    def test_create_chunks(self):
        start_date = dt.datetime(month=1, day=1, year=2015)
        end_date = dt.datetime(month=1, day=1, year=2023)
        test = FreqChunker(start_date, end_date, 50, 25)
        self.fail()
