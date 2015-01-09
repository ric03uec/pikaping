import logging
import pika
import time
import json
import uuid
import threading

class Exchange(object):

    EXCHANGE_TYPE = 'topic'
    QUEUE = None

    def __init__(self, url, listen_queue_name, callback, requeueOnFailure, config):
        logging.basicConfig(level=logging.DEBUG)
        self.log = logging.getLogger(__name__)
        self.connection = None
        self.channel = None
        self.closing = False
        self.consumer_tag = None
        self.config = config
        self.url = url #self.config["AMQP_URL"]
        self.LISTEN_QUEUE = self.config['LISTEN_QUEUE']
        self.EXCHANGE = self.config["DEFAULT_EXCHANGE"]
        self.log.info('url {0}'.format(self.url))
        self.is_initialized = False
        self.should_ping = threading.Event()
        self.ping_secs = self.config['PING_TIMEOUT']

        self._callback = callback
        self._requeueOnFailure = requeueOnFailure

    def connect(self):
        new_connection = None
        try:
            self.log.info('Connecting to {0}'.format(self.url))
            param = parameters = pika.URLParameters(self.url)

            new_connection = pika.SelectConnection(param,
                                                   self.on_connection_open,
                                                   stop_ioloop_on_close=False)

        except pika.exceptions.AMQPConnectionError, e:
            self.log.warn('Pika Connection error ' + str(e))

        return new_connection

    def close_connection(self):
        self.log.debug('Closing connection')
        self.connection.close()

    def add_on_connection_close_callback(self):
        self.log.debug('Adding connection close callback')
        self.connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self.channel = None
        if self.closing:
            self.connection.ioloop.stop()
        else:
            self.log.warn('Connection closed, reopening in 5 seconds')
            self.connection.ioloop.stop()
            #self.connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        self.log.debug('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self.connection.ioloop.stop()

        if not self.closing:
            # Create a new connection
            self.connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            if self.connection is not None:
                self.connection.ioloop.start()

    def add_on_channel_close_callback(self):
        self.log.debug('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.log.warn('Channel {0} was closed: ({1}) {2}'.format(channel, reply_code, reply_text))
        self.connection.close()

    def on_channel_open(self, channel):
        self.log.debug('Channel opened')
        try:
            channel.basic_qos(prefetch_count=1)
            #print("QOS works!!")
        except Exception as e:
            self.log.error('Error setting qos', exc_info=e)
        self.channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        self.log.debug('Declaring exchange {0}'.format(exchange_name))
        #print('Declaring exchange %s' % exchange_name)
        self.channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE,
                                       durable=True)

    def on_exchange_declareok(self, unused_frame):
        self.log.debug('Exchange declared')
        self.setup_queue(self.LISTEN_QUEUE)

    def setup_queue(self, queue_name):
        self.log.debug('Initializing queue {0}'.format(queue_name))
        self.channel.queue_declare(self.on_queue_declare_ok, queue_name, durable=True)

    def on_queue_declare_ok(self, method_frame):
        self.log.debug('Binding common queue {0} to {1} with {2}'
                .format(self.EXCHANGE, self.LISTEN_QUEUE, self.LISTEN_QUEUE))

        self.channel.queue_bind(self.on_bindok, self.LISTEN_QUEUE,
                                 self.EXCHANGE, self.LISTEN_QUEUE)

    def on_bindok(self, unused_frame):
        self.log.debug('Common Queue bound {0}'.format(self.LISTEN_QUEUE))
        self.start_consuming()

    def add_on_cancel_callback(self):
        self.log.debug('Adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        self.log.debug('Consumer was cancelled remotely, shutting down: {0}'
                          .format(method_frame))
        if self.channel:
            self.channel.close()

    def acknowledge_message(self, delivery_tag):
        self.log.debug('Acknowledging message {0}'.format(delivery_tag))
        self.channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag):
        self.log.debug('Rejecting message {0} with requeue {1}'
                .format(delivery_tag, self._requeueOnFailure))
        self.channel.basic_reject(delivery_tag, self._requeueOnFailure)

    def requeue_prov_message(self, delivery_tag):
        self.log.debug('Requeueing message {0} with requeue True'
                .format(delivery_tag))
        self.channel.basic_reject(delivery_tag, True)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        self.log.debug('Received message # {0} from {1}: {2}'
                .format(basic_deliver.delivery_tag, properties.app_id, body))
        is_redelivered = basic_deliver.redelivered
        isSuccess = False
        response = {}
        try:
            thread = threading.Thread(target=self.ping)
            thread.start()
            response = self._callback(body, is_redelivered)
            isSuccess = response['success']
        except Exception as e:
            self.log.error('Exception in message handler callback', exc_info=e)
        finally:
            self.should_ping.set()

        self.log.debug('Callback reported success: {0}'.format(isSuccess))
        if (isSuccess):
            self.acknowledge_message(basic_deliver.delivery_tag)
        else:
            requeue_prov = response.get('requeue_prov', False)
            if requeue_prov:
                self.requeue_prov_message(basic_deliver.delivery_tag)
            else:
                self.reject_message(basic_deliver.delivery_tag)

    def ping(self):
        self.log.debug("RabbitMQ ping thread started for listener")
        while not self.should_ping.is_set():
            self.log.debug('Pinging on "ping" queue')
            self.should_ping.wait(self.ping_secs)
            self.channel.basic_publish(exchange=self.EXCHANGE,
                                        routing_key="ping", body=json.dumps({"ping": "ping"}),
                                        properties=pika.BasicProperties(message_id=str(uuid.uuid4())))
        self.log.debug("RabbitMQ ping thread quitting for listener")

    def on_cancelok(self, unused_frame):
        self.log.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        if self.channel:
            self.log.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self.channel.basic_cancel(self.on_cancelok, self.consumer_tag)

    def start_consuming(self):
        self.log.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self.consumer_tag = self.channel.basic_consume(self.on_message,
                                                         self.LISTEN_QUEUE)

    def close_channel(self):
        self.log.debug('Closing the channel')
        self.channel.close()

    def open_channel(self):
        self.log.debug('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        self.connection = self.connect()
        try:
            self.log.debug('Starting IO loop')
            #print('Starting IO loop')
            if self.connection is not None:
                self.connection.ioloop.start()
                self.log.error('Error listening to queue')
            #print('IO loop started')
        except Exception as e:
            #print('IO LOOP EXCEPTION')
            if not self.closing:
                self.log.warn('Unhandled exception! Pika disconnected', exc_info=e)
                self.stop()

    def stop(self):
        self.log.debug('Stopping')
        self.closing = True
        self.stop_consuming()
        #self.connection.ioloop.start()
        self.log.debug('Stopped')
