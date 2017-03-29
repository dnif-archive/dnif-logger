import unittest

from dnif.consumer import Consumer
from dnif.logger import DnifLogger


class FakeConsumer(Consumer):
    def __init__(self):
        self.data_list = []

    def send(self, data):
        self.data_list.append(data)


class TestDnifLogger(unittest.TestCase):
    def test_log(self):
        consumer = FakeConsumer()
        payload = {'a': 1}
        dlog = DnifLogger(consumer)
        dlog.log(payload)
        self.assertTrue(payload == consumer.data_list[0])
