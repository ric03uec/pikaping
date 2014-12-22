import logging
import pika
import time
import json
import uuid

class Exchange(object):

    EXCHANGE_TYPE = 'topic'
    QUEUE = None

    def __init__(self, config, url, listen_queue_name, publish_queue_name, callback, requeueOnFailure):
        self.setupLog()

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self.config = config
        self._url = url
        self.EXCHANGE = self.config["DEFAULT_EXCHANGE"]
        self._logger.info('url {0}'.format(self._url))
        self.INIT_QUEUES = []
        self.PUBLISH_STATUS_QUEUE = None
        self.LISTEN_PROV_QUEUE = None
        self.is_initialized = False

    

        if publish_queue_name is not None:
            self.PUBLISH_STATUS_QUEUE = publish_queue_name.lower()
            self.INIT_QUEUES.append(self.PUBLISH_STATUS_QUEUE)

        if listen_queue_name is not None:
            self.LISTEN_PROV_QUEUE = listen_queue_name.lower()
            self.INIT_QUEUES.append(self.LISTEN_PROV_QUEUE)

        self.CREATING_QUEUES = list(self.INIT_QUEUES)
        self._callback = callback
        self._requeueOnFailure = requeueOnFailure

    def setupLog(self):
        self._logger = logging.getLogger()

        logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)

        self._logger.addHandler(consoleHandler)

        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = True

    def connect(self):
        new_connection = None
        try:
            self._logger.info('Connecting to %s', self._url)
            param = parameters = pika.URLParameters(self._url)

            new_connection = pika.SelectConnection(param,
                                                   self.on_connection_open,
                                                   stop_ioloop_on_close=False)

            #print('Successfully connected to %s' % self._url)

        except pika.exceptions.AMQPConnectionError, e:
            #print('Connection error. Waiting for 5 seconds and trying again ' + str(e))
            self._logger.warn('Pika Connection error ' + str(e))
            #self._logger.debug('Connection error. Waiting for 5 seconds and trying again')

        return new_connection

    def close_connection(self):
        self._logger.debug('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        self._logger.debug('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    # TODO: Uncomment this when we upgrade to pika 0.9.13+
    # def on_connection_closed(self, connection, reply_code, reply_text):
    def on_connection_closed(self, method_frame):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._logger.warning('Connection closed, reopening in 5 seconds')
            self._connection.ioloop.stop()
            #self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        self._logger.debug('Connection opened')
        #print('Connection opened')
        self.add_on_connection_close_callback()
        #print('processed on_connection_close_callback')
        self.open_channel()
        #print('processed open_channel')

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            if self._connection is not None:
                self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        self._logger.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self._logger.warning('Channel %i was closed: (%s) %s',
                             channel, reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        self._logger.debug('Channel opened')
        try:
            channel.basic_qos(prefetch_count=1)
            #print("QOS works!!")
        except Exception as e:
            self._logger.error('Error setting qos', exc_info=e)
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        self._logger.debug('Declaring exchange %s', exchange_name)
        #print('Declaring exchange %s' % exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE,
                                       durable=True)

    def on_exchange_declareok(self, unused_frame):
        self._logger.debug('Exchange declared')
        #print('Exchange declared')
        self.setup_queues()

    def setup_queues(self):
        self._logger.debug('Initializing queue length %d', len(self.CREATING_QUEUES))
        if len(self.CREATING_QUEUES) == 0:
            return
        key = self.CREATING_QUEUES[0].lower()

        self._logger.debug('Declaring common queue %s', key)
        #print('Declaring common  queue %s' % key)
        self._channel.queue_declare(self.on_queue_declare_ok, key, durable=True)
        self._logger.debug('AFTER Declaring common queue %s' % key)

    def on_queue_declare_ok(self, method_frame):
        key = self.CREATING_QUEUES[0].lower()
        self._logger.debug('Binding common queue %s to %s with %s',
                          self.EXCHANGE, key, key)

        #print('Binding common queue ' + self.EXCHANGE + ' to ' + key + ' with ' + key + " : " + str(method_frame))

        self._channel.queue_bind(self.on_queue_bind_ok, key,
                                 self.EXCHANGE, key)

        #print('AFTER Binding common queue ' + self.EXCHANGE + ' to ' + key + ' with ' + key)

    def on_queue_bind_ok(self, unused_frame):
        key = self.CREATING_QUEUES[0].lower()

        self._logger.debug('Common Queue bound %s', key)

        self.CREATING_QUEUES.pop(0)

        if len(self.CREATING_QUEUES) == 0:
            self.start_consuming()
        else:
            self.setup_queues()

    def add_on_cancel_callback(self):
        self._logger.debug('Adding consumer cancellation callback')
        #print('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        self._logger.debug('Consumer was cancelled remotely, shutting down: %r',
                          method_frame)
        if self._channel:
            self._channel.close()

    def publish_message(self, message):
        self._logger.debug('Publishing Message %s \n\t RK: %s' % (message, self.PUBLISH_STATUS_QUEUE,))
        #print('Publishing Message to exchange ' + self.EXCHANGE + ' QUEUE ' + self.PUBLISH_STATUS_QUEUE)
        self._channel.basic_publish(exchange=self.EXCHANGE, \
                                    routing_key=self.PUBLISH_STATUS_QUEUE, body=json.dumps(message), \
                                    properties=pika.BasicProperties(message_id=str(uuid.uuid4())))

    def ping(self):
        self._logger.debug('Pinging')
        #print('Publishing Message to exchange ' + self.EXCHANGE + ' QUEUE ' + self.PUBLISH_STATUS_QUEUE)
        self._channel.basic_publish(exchange=self.EXCHANGE,
                                    routing_key="ping", body=json.dumps({"ping": "ping"}),
                                    properties=pika.BasicProperties(message_id=str(uuid.uuid4())))

    def acknowledge_message(self, delivery_tag):
        self._logger.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag):
        self._logger.debug('Rejecting message %s with requeue %s', delivery_tag, self._requeueOnFailure)
        self._channel.basic_reject(delivery_tag, self._requeueOnFailure)

    def requeue_prov_message(self, delivery_tag):
        self._logger.debug('Requeueing message %s with requeue True', delivery_tag)
        self._channel.basic_reject(delivery_tag, True)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        self._logger.debug('Received message # %s from %s: %s',
                          basic_deliver.delivery_tag, properties.app_id, body)
        is_redelivered = basic_deliver.redelivered
        isSuccess = False
        response = {}
        try:
            response = self._callback(body, is_redelivered)
            isSuccess = response['success']
        except Exception as e:
            self._logger.error('Exception in message handler callback', exc_info=e)

        self._logger.debug('Callback reported success: {0}'.format(isSuccess))
        if (isSuccess):
            self.acknowledge_message(basic_deliver.delivery_tag)
        else:
            requeue_prov = response.get('requeue_prov', False)
            if requeue_prov:
                self.requeue_prov_message(basic_deliver.delivery_tag)
            else:
                self.reject_message(basic_deliver.delivery_tag)

    def on_cancelok(self, unused_frame):
        self._logger.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        if self._channel:
            self._logger.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        self._logger.debug('Issuing consumer related RPC commands')
        if self.LISTEN_PROV_QUEUE is not None:
            self.add_on_cancel_callback()
            self.is_initialized = True
            self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.LISTEN_PROV_QUEUE)

    def close_channel(self):
        self._logger.debug('Closing the channel')
        self._channel.close()

    def open_channel(self):
        self._logger.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)
        #print('created channel')

    def run(self):
        self._connection = self.connect()
        try:
            self._logger.debug('Starting IO loop')
            #print('Starting IO loop')
            if self._connection is not None:
                self._connection.ioloop.start()
                self._logger.error('Error listening to queue')
            #print('IO loop started')
        except Exception as e:
            #print('IO LOOP EXCEPTION')
            if not self._closing:
                self._logger.warn('Unhandled exception! Pika disconnected', exc_info=e)
                self.stop()

    def stop(self):
        self._logger.debug('Stopping')
        self._closing = True
        self.stop_consuming()
        #self._connection.ioloop.start()
        self._logger.debug('Stopped')
