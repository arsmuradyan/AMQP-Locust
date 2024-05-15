from datetime import datetime
import logging
import multiprocessing
import string
import threading
import random

import pika

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)


class RabbitMQClient(object):
    _connected = False

    def __init__(self, parameters: pika.URLParameters, **kwargs):
        self._process_name = multiprocessing.current_process().name
        self._thread_name = threading.current_thread().name
        self._parameters = parameters
        self._exchange = kwargs["exchange"]
        self._routing_key = kwargs["routing_key"]
        self._delivery_mode = kwargs["delivery_mode"]
        self._request_event = kwargs["request_event"]

    def connect(self):
        self._connection = pika.BlockingConnection(self._parameters)
        self._channel = self._connection.channel()
        self._connected = True

    @staticmethod
    def get_random_string(length: int = 0) -> str:
        letters = string.ascii_lowercase
        return "".join(random.choice(letters) for i in range(length))

    def publish(self):
        """
        Constructs and publishes a simple message
        via amqp.basic_publish

        """
        if not self._connected:
            self.connect()
        # if not self._channel.delivery_confirmation:
        #     self._channel.confirm_delivery()
        message = RabbitMQClient.get_random_string(1235)
        watch = StopWatch()
        try:
            watch.start()
            self._channel.basic_publish(
                self._exchange,
                self._routing_key,
                message,
                pika.BasicProperties(
                    content_type="text/plain", delivery_mode=self._delivery_mode
                ),
            )
            watch.stop()
        except Exception as e:
            watch.stop()
            self._request_event.fire(
                request_type="BASIC_PUBLISH",
                name=self._routing_key,
                response_time=watch.elapsed_time(),
                exception=e,
                response_length=0,
            )
            self.connect()
        else:
            self._request_event.fire(
                request_type="BASIC_PUBLISH",
                name=self._routing_key,
                response_time=watch.elapsed_time(),
                response_length=0,
            )

    def close_channel(self):
        if self._channel is not None:
            LOGGER.info("Closing the channel")
            self._channel.close()

    def close_connection(self):
        if self._connection is not None:
            LOGGER.info("Closing connection")
            self._connection.close()

    def disconnect(self):
        self.close_channel()
        self.close_connection()
        self._connected = False


class StopWatch:
    def start(self):
        self._start = datetime.now()

    def stop(self):
        self._end = datetime.now()

    def elapsed_time(self):
        timedelta = self._end - self._start
        return timedelta.total_seconds() * 1000
