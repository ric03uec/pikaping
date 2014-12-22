import time
from exchange import Exchange
from config import config

def queue_callback(message, is_redelivered):
    print '================================================'
    print '================================================'
    print 'message received'
    print '================================================'
    print message

    # acknowledge this after a long time
    time.sleep(config['PROCESS_EXEC_TIME'])

    response = {}
    response['success'] = config['PROCESS_EXEC_RETURN']

    return response

if __name__ == '__main__':
    exConfig = {}
    exConfig['DEFAULT_EXCHANGE'] = 'test.exchange'

    requeueOnFailure = config['REQUEUE_ON_FAILURE']

    exchange = Exchange(exConfig,
            config['URL'], config['LISTEN_QUEUE'], 
            None, queue_callback,
            requeueOnFailure)

    exchange.run()
