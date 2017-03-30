import logging
import socket
import threading
from Queue import Queue, Full, Empty

import requests
import time
from dnif.exception import DnifException


class Consumer(object):
    def start(self, data, **kwargs):
        raise NotImplementedError

    def stop(self, data, **kwargs):
        raise NotImplementedError

    def send(self, data):
        raise NotImplementedError


class AsyncBufferedConsumer(Consumer):
    """ (Abstract) Consumer that uploads logs asynchronously in the background . """

    def __init__(self, buffer_size=1024):
        """
        Initialize the Consumer
        :param buffer_size: Max number of pending payloads to hold (in memory).
        If the queue is full, further payloads will be dropped
        """
        self._queue = Queue(maxsize=buffer_size)
        self._logger = logging.getLogger('dnif.consumer.' + self.__class__.__name__)
        self._stop = self._force_stop = True
        self._thread = None

    def start(self, daemon=False, **kwargs):
        """ Start uploading. Running in daemon mode could cause data loss!
        :param daemon: if this is true, background thread will stop automatically with program completion
          without the need to explicitly call stop()
        """
        if self._thread and self._thread.isAlive():
            raise DnifException('already running')

        self._stop = self._force_stop = False
        self._thread = threading.Thread(target=self.upload)
        self._thread.daemon = bool(daemon)
        self._thread.start()

    def stop(self, force=False, **kwargs):
        """ Stop uploading. Forcing stop could cause data loss!
        :param force: stops upload immediately, dropping any pending uploads
        """
        self._stop = True
        self._force_stop = force

    def send(self, data):
        """ Send the data to the target endpoint.
        This method only queues the upload, the upload itself happens asynchronously in the background.

        :param data: Data to upload
        """
        if self._stop:
            # don't add to the worker thread's burden after stop has been signalled
            return

        data = self.validate(data)
        if not data:
            return

        try:
            self._queue.put(data, block=False)
        except Full:
            self._logger.info('Dropping data because max buffer size reached: {0}'.format(data))

    def validate(self, data):
        """
        Validate the data packet, and return the validated packet
        :param data: the data packet to send
        """
        raise NotImplementedError

    def upload(self):
        """
        Actually upload
        """
        raise NotImplementedError


class AsyncHttpConsumer(AsyncBufferedConsumer):
    """ Consumer that uploads logs to the specified endpoint using HTTP. """

    def __init__(self, url, buffer_size=1024, batch_size=100):
        """
        :param url: The target URL
        :param buffer_size: Max number of pending payloads to hold (in memory).
        """
        self._url = url
        self._timeout = 15
        self._batch_size = batch_size
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
            self._logger.info('Skipping sending data packet. Data must be list/dict: {0}'.format(data))
            return None

        if isinstance(data, dict):
            data = [data]

        final = []
        for d in data:
            if self._validate_unit(d):
                final.append(d)

        return final

    def upload(self):
        while not self._force_stop:
            contents = []
            try:
                for i in range(self._batch_size):
                    data = self._queue.get_nowait()
                    if isinstance(data, list):
                        contents.extend(data)
                    else:
                        contents.append(data)
            except Empty:
                pass

            if not contents:
                if self._stop:
                    # loop's primary exit condition
                    break
                time.sleep(1)
            else:
                # check force stop flag again before making request
                if self._force_stop:
                    break

                try:
                    resp = requests.post(self._url, json=contents, timeout=self._timeout, verify=False)
                    # self._logger.debug(resp.status_code)
                except Exception as ex:
                    self._logger.info('Error uploading log: {0}'.format(ex))

        self._logger.info('Background uploader stopped.')


class AsyncUDPConsumer(AsyncBufferedConsumer):
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
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while not self._force_stop:
                try:
                    message = self._queue.get(block=True, timeout=1)

                    # check force stopped flag again before making request
                    if self._force_stop:
                        break

                    try:
                        sock.sendto(message, (self._target_ip, self._target_port))
                    except Exception as ex:
                        self._logger.info('Error uploading log: {0}'.format(ex))
                except Empty:
                    if self._stop:
                        break  # loop's primary exit condition

        self._logger.info('Background uploader stopped.')
