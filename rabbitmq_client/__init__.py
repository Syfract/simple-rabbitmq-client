import puka
from multiprocessing import Process


class RabbitMQClient:
    def __init__(self, url):
        self.client = puka.Client(url)
        promise = self.client.connect()
        self.client.wait(promise)
        self.p = None

    def push(self, message, queue, exchange=''):
        promise = self.client.queue_declare(queue=queue, durable=True)
        self.client.wait(promise)
        promise = self.client.basic_publish(exchange=exchange, routing_key=queue, body=message)
        self.client.wait(promise)

    def pull(self, callback, queue):
        promise = self.client.queue_declare(queue=queue, durable=True)
        self.client.wait(promise)
        consume_promise = self.client.basic_consume(queue=queue, prefetch_count=1)
        self.p = Process(target=self._handle_pull, args=(consume_promise, callback))
        self.p.start()

    def _handle_pull(self, consume_promise, callback):
        while True:
            result = self.client.wait(consume_promise)
            callback(result)

    def delete(self, message, ack=True):
        if ack:
            self.client.basic_ack(message)
        else:
            self.client.basic_reject(message)


# For backwards compatibility
RabbitmqClient = RabbitMQClient
