import unittest

from dnif.consumer import Consumer
from dnif.logger import DnifLogger


class FakeConsumer(Consumer):
    def __init__(self):
        self.data_list = []
        self.start_called = False
        self.stop_called = False

    def start(self, **kwargs):
        self.start_called = True

    def stop(self, **kwargs):
        self.stop_called = True

    def send(self, data):
        self.data_list.append(data)


class TestDnifLogger(unittest.TestCase):
    def test_start(self):
        consumer = FakeConsumer()
        DnifLogger(consumer).start()
        DnifLogger(consumer).stop()
        self.assertTrue(consumer.start_called)

    def test_stop(self):
        consumer = FakeConsumer()
        DnifLogger(consumer).start()
        DnifLogger(consumer).stop()
        self.assertTrue(consumer.stop_called)

    def test_log(self):
        consumer = FakeConsumer()
        payload = {'a': 1}
        dlog = DnifLogger(consumer)
        dlog.log(payload)
        self.assertTrue(payload == consumer.data_list[0])
