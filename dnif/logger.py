class DnifLogger(object):
    """
    The logging utility class that provides the `send` interface.
    This abstracts the actual upload implementation to provide for
    a transparent way to plug in different implementations.
    """
    def __init__(self, consumer):
        """
        :param consumer: dnif.consumer.Consumer
        """
        self._consumer = consumer

    def log(self, data):
        self._consumer.send(data)
