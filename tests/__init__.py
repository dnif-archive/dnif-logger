import time

from dnif.consumer import HttpConsumer
from dnif.logger import DnifLogger

dlog = DnifLogger(HttpConsumer('http://localhost:5000/json/hello'))
dlog.log({'a': 1, 'b': 'x', 'c': 12.345})

for i in range(10):
    dlog.log({'a': 1, 'b': 'x', 'c': 12.345})
    time.sleep(1)
    print('i: {0}'.format(i))

dlog

pass
