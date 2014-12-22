import json
import uuid
import pika
from config import config

def connect_and_publish(message, key):
    """This connects to RabbitMQ with a blocking connection, sends 
    messages from the (message, routing key) tuples in the messages list, 
    and closes the connection.

    """

    channel = None
    try:
        print('Connecting to %s',config['URL'])
        parameters = pika.URLParameters(config['URL'])
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        print('Publishing Message %s \n\t RK: %s' % (message, key,))
        channel.basic_publish(exchange='test.exchange', \
                                routing_key=key, body=json.dumps(message), \
                                properties=pika.BasicProperties(message_id=str(uuid.uuid4())))

        print('Closing connection')
        connection.close()

    except pika.exceptions.AMQPConnectionError, e:
        print('Could not connect to ' + self._url)
        raise e

if __name__ == '__main__':
    message = {
        'messageData' : "somerandomdata"
    }
    connect_and_publish(message, config['LISTEN_QUEUE'])
