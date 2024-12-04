from typing import Callable, Union, Iterable
from pika import BlockingConnection, URLParameters, exceptions

__all__ = ['RabbitMQClient']


def recover_connection(func):
    def inner(self, *args, **kwargs):
        # Connect if not already connected
        if self.connection is None or not self.connection.is_open:
            self._connect()

        while True:
            try:
                func(self, *args, **kwargs)
                break
            # Reconnect if the channel was closed
            except exceptions.ChannelWrongStateError as e:
                print("Channel is in wrong state: %s! Reconnecting..." % str(e))
                self._connect()
            # Don't recover if connection was closed by broker or on channel errors
            except (exceptions.ConnectionClosedByBroker, exceptions.AMQPChannelError) as e:
                print("Unrecoverable error occurred!")
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
    connection = None
    client = None
    process = None

    def __init__(self, url, connect_now=False):
        self.url = url
        if connect_now:
            self._connect()

    @recover_connection
    def push(self, message: str, queue: str, exchange: str = ''):
        self.client.queue_declare(queue=queue, durable=True)
        self.client.basic_publish(exchange=exchange, routing_key=queue, body=message.encode('utf-8'))

    @recover_connection
    def pull(self, callback: Callable, queue: Union[str, Iterable[str]], prefetch_count: int = 1):
        if type(queue) == str:
            queue = (queue,)

        channel = self.client

        channel.basic_qos(prefetch_count=prefetch_count)

        for q in queue:
            channel.basic_consume(q, callback)

        try:
            channel.start_consuming()
        except (KeyboardInterrupt, InterruptedError):
            channel.stop_consuming()

    @recover_connection
    def delete(self, message, ack=True):
        if ack:
            self.client.basic_ack(message)
        else:
            self.client.basic_nack(message)

    @recover_connection
    def ack(self, tag):
        self.client.basic_ack(tag)

    def _connect(self):
        self.connection = BlockingConnection(URLParameters(self.url))
        self.client = self.connection.channel()
