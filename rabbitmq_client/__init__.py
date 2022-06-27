from typing import Callable, Union, Iterable
from pika import BlockingConnection, URLParameters, exceptions

__all__ = ['RabbitMQClient']


def recover_connection(func):
    def inner(self, *args, **kwargs):
        while True:
            try:
                func(self, *args, **kwargs)
                break
            # Don't recover if connection was closed by broker or on channel errors
            except (exceptions.ConnectionClosedByBroker, exceptions.AMQPChannelError) as e:
                raise e
            # Recover on all other connection errors
            except exceptions.AMQPConnectionError as e:
                print("Encountered a connection error: %s! Recovering..." % str(e))
                try:
                    self.connection.close()
                except exceptions.ConnectionWrongStateError:
                    pass
                self._connect()
    return inner


class RabbitMQClient:
    url = ""
    client = None
    process = None

    def __init__(self, url):
        self.url = url
        self._connect()

    @recover_connection
    def push(self, message: str, queue: str, exchange: str = ''):
        self.client.queue_declare(queue=queue, durable=True)
        self.client.basic_publish(exchange=exchange, routing_key=queue, body=message.encode('utf-8'))

    @recover_connection
    def pull(self, callback: Callable, queue: Union[str, Iterable[str]]):
        if type(queue) == str:
            queue = (queue,)

        channel = self.client

        for q in queue:
            channel.basic_consume(q, callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

    @recover_connection
    def delete(self, message, ack=True):
        if ack:
            self.client.basic_ack(message)
        else:
            self.client.basic_nack(message)

    def _connect(self):
        self.connection = BlockingConnection(URLParameters(self.url))
        self.client = self.connection.channel()
