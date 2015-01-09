import time
import threading
from exchange import Exchange
from config import config

def queue_callback(message, is_redelivered):
    print '================================================'
    print '================================================'
    print 'message received'
    print '================================================'
    print message
    print 'setting up ping thread'
    should_ping = threading.Event()

    thread = threading.Thread(target=ping)
    thread.start()
    print 'ping thread started...'

    time.sleep(config['PROCESS_EXEC_TIME'])
    response = {}
    response['success'] = config['PROCESS_EXEC_RETURN']

    return response

def ping(exchange, should_ping):
    print("RabbitMQ ping thread started")
    while not should_ping.is_set():
        exchange.ping()
        should_ping.wait(30)
    print("RabbitMQ ping thread quitting")

if __name__ == '__main__':
    exConfig = {}
    exConfig['DEFAULT_EXCHANGE'] = 'test.exchange'

    requeueOnFailure = config['REQUEUE_ON_FAILURE']

    exchange = Exchange(exConfig,
            config['URL'], config['LISTEN_QUEUE'], 
            None, queue_callback,
            requeueOnFailure)

    exchange.run()
