# dnif-logger
This is the official Python DNIF client library. This allows you to directly write log statements into your application code and integrate it with DNIF Adapters.

## Installation
You can download the library using pip, as:
```sh
$ pip install dnif-logger
```

## Getting Started
The DNIF library currently supports two ingestion mechanisms:
1. TCP protocol uploads using the HTTP(S) endpoint
2. Uploads using the UDP protocol

Typical usages for both of these are given below.

### TCP
```python
from dnif.consumer import AsyncHttpConsumer
from dnif.logger import DnifLogger
dlog = DnifLogger(AsyncHttpConsumer('http://TARGET_IP:PORT/json/receive'))
dlog.log({'key': 'value'})
```

The AsyncHttpConsumer is thread-safe, so you can use the same instance
across threads (recommended).

### UDP
```python
# coming soon
```
