config = {
        'URL' : "amqp://guest:guest@172.17.42.1:5672/%2F?heartbeat_interval=5",
    'LISTEN_QUEUE' : 'listen.queue',
    'ADD_CONNECTION_TIMEOUT' : False,
    'PROCESS_EXEC_TIME' : 4,
    'PROCESS_EXEC_RETURN' : False,
    'REQUEUE_ON_FAILURE' : True,
    'DEFAULT_EXCHANGE' : 'shippableEx',
    'PING_TIMEOUT' : 1
}
    #'URL' : "amqps://C8CA856E81BCD478:F988AE403A3791AA@msg.shippablelocal.com:2727/shippable",
#'URL' : "amqps://F7694B66323B03AB:50FF4551B14AF719@172.17.42.1:5671/shippable?socket_timeout=120&heartbeat_interval=30",
# 'URL' : "amqps://guest:guest@msg.shippablelocal.com:2727/%2F?socket_timeout=120&heartbeat_interval=30",
