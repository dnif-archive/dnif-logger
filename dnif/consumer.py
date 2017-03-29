import logging
import socket
import threading
from Queue import Queue, Full

import requests


class Consumer(object):
    def send(self, data):
        raise NotImplementedError


class AsyncConsumer(Consumer):
    """ (Abstract) Consumer that uploads logs asynchronously in the background . """

    def __init__(self, buffer_size=1024):
        """
        Initialize the Consumer
        :param buffer_size: Max number of pending payloads to hold (in memory).
        If the queue is full, further payloads will be dropped
        """
        self._queue = Queue(maxsize=buffer_size)
        self._logger = logging.getLogger('dnif.consumer.' + self.__class__.__name__)

        # TODO: This shouldn't be a Daemon and should listen for shutdown events.
        self._thread = threading.Thread(target=self.upload)
        self._thread.daemon = True
        self._thread.start()

    def validate(self, data):
        """
        Validate the data packet, and return the validated packet
        :param data: the data packet to send
        :return:
        """
        raise NotImplementedError

    def send(self, data):
        """ Send the data to the target endpoint.
        This method only queues the upload, the upload itself happens asynchronously in the background.

        :param data: Data to upload
        """
        data = self.validate(data)
        if not data:
            return

        try:
            self._queue.put(data, block=False)
        except Full:
            self._logger.info('Dropping data because max buffer size reached: {0}'.format(data))

    def upload(self):
        """
        Actually upload
        """
        raise NotImplementedError


class AsyncHttpConsumer(AsyncConsumer):
    """ Consumer that uploads logs to the specified endpoint using HTTP. """

    def __init__(self, url, buffer_size=1024):
        """
        :param url: The target URL
        :param buffer_size: Max number of pending payloads to hold (in memory).
        """
        self._url = url
        self._timeout = 15
        super(AsyncHttpConsumer, self).__init__(buffer_size)

    def _validate_unit(self, data):
        """ Validate that data does not contain nested JSON objects. Lists are allowed. """
        for key, value in data.items():
            if isinstance(value, dict):
                self._logger.info('Skipping sending data packet. Nested JSON objects are not allowed: {0}'.format(data))
                return False
        return True

    def validate(self, data):
        if not isinstance(data, (dict, list, tuple)):
            self._logger.info('Skipping sending data packet. Data must be listobject: {0}'.format(data))
            return

        if isinstance(data, dict):
            data = [data]

        final = []
        for d in data:
            if self._validate_unit(d):
                final.append(d)

        return final

    def upload(self):
        while True:
            # TODO: Batch payloads and send as one request
            payload = self._queue.get(block=True)
            try:
                resp = requests.post(self._url, json=payload, timeout=self._timeout)
                # self._logger.debug(resp.status_code)
            except Exception as ex:
                self._logger.info('Error uploading log: {0}'.format(ex))


class AsyncUDPConsumer(AsyncConsumer):
    """ Consumer that uploads logs to the specified endpoint using UDP """

    def __init__(self, target_ip, target_port, buffer_size=1024):
        """
        Initialize
        :param target_ip: The target UDP ip
        :param target_port: The target UDP port
        :param buffer_size: Max number of pending payloads to hold (in memory).
        """
        self._target_ip = target_ip
        self._target_port = target_port
        super(AsyncUDPConsumer, self).__init__(buffer_size)

    def validate(self, data):
        if isinstance(data, (str, unicode)):
            return data
        else:
            self._logger.info('Skipping sending data packet. Expected str/unicode: {0}'.format(data))
            return None

    def upload(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            message = self._queue.get(block=True)
            try:
                sock.sendto(message, (self._target_ip, self._target_port))
            except Exception as ex:
                self._logger.info('Error uploading log: {0}'.format(ex))
