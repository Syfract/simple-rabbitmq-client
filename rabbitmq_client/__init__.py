from typing import Callable
import pika

__all__ = ['RabbitMQClient']


def recover_connection(func):
    def inner(self, *args, **kwargs):
        while True:
            try:
                func(self, *args, **kwargs)
                break
            # Don't recover if connection was closed by broker or on channel errors
            except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPChannelError) as e:
                raise e
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError as e:
                print("Encountered a connection error: %s! Recovering..." % str(e))
                try:
                    self.connection.close()
                except pika.exceptions.ConnectionWrongStateError:
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
        self.client.basic_publish(exchange=exchange, routing_key=queue, body=message)

    @recover_connection
    def pull(self, callback: Callable, queue: str):
        channel = self.client
        channel.basic_consume(queue, callback)
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
        self.connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self.client = self.connection.channel()
