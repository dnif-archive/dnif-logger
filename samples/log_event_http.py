import logging

from dnif.consumer import AsyncHttpConsumer
from dnif.logger import DnifLogger

logging.basicConfig(level=logging.DEBUG, filename='dnif.log', filemode='a')

url = 'http://TARGET_IP:PORT/json/receive'
max_buffer_size = 1024  # optional

# Initialize DNIF logger using the HTTP Consumer
dlog = DnifLogger(AsyncHttpConsumer(url, buffer_size=max_buffer_size))

# Send single log statement
payload = {
    'key1': 'value1',
    'key2': 2,
    'key3': 3.12345,
    'key4': ['x', 'y', 'z'],
}
dlog.log(payload)

# Send multiple log statements at once (bulk send).
# Order matters. The events will be recorded in the order received.
payload = [
    {'key1': 1},
    {'key1': 2},
    {'key1': 3},
]
dlog.log(payload)
