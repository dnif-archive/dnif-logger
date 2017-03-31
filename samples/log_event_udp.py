import logging

from dnif.consumer import AsyncUDPConsumer
from dnif.logger import DnifLogger

logging.basicConfig(level=logging.DEBUG, filename='dnif.log', filemode='a')

udp_ip = 'TARGET_IP'
udp_port = 1234  # TARGET PORT
max_buffer_size = 1024  # optional

# Initialize DNIF logger using the UDP Consumer
dlog = DnifLogger(AsyncUDPConsumer(udp_ip, udp_port, buffer_size=max_buffer_size))
dlog.start()

# Send single log statement
dlog.log('Hello World')

# Stop. This is *required* when using an async consumer.
dlog.stop(force=False)
