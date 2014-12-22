import time
from exchange import Exchange
from config import config

def queue_callback(message, is_redelivered):
    print 'callback called'
    print message

    # acknowledge this after a long time
    time.sleep(config['PROCESS_EXEC_TIME'])

    response = {}
    response['success'] = True

    return response

if __name__ == '__main__':
    exConfig = {}
    exConfig['DEFAULT_EXCHANGE'] = 'test.exchange'

    requeueOnFailure = False

    exchange = Exchange(exConfig,
            config['URL'], config['LISTEN_QUEUE'], 
            None, queue_callback,
            requeueOnFailure)

    exchange.run()
