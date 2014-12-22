pikaping
========

testing pika connection with a dummy receiver and publisher

- `config.py` contains the test values for pika connection timeout, worker time and requeue on failure
- receiver boots up an exchange and attaches a receiver to it
- receiver adds a callback to the exchange on the queue specified in config
- the callback returns True/False to the exchange after specified timeout(which is dummy process time)
