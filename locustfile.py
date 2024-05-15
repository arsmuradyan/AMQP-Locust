from locust import HttpUser, task, between, events
import pika
from rabbitmq import RabbitMQClient


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--exchange", type=str, default="")
    parser.add_argument("--routing-key", type=str, default="")
    parser.add_argument("--delivery-mode", choices=["1", "2"], default="1", )
    parser.add_argument("--username", type=str, default="")
    parser.add_argument("--password", type=str, is_secret=True, default="")


class RabbitMQUser(HttpUser):
    wait_time = between(1, 5)  # Time between tasks in seconds

    def on_start(self):
        # Establish a connection to RabbitMQ
        delivery_mode = self.environment.parsed_options.delivery_mode == "1" and pika.DeliveryMode.Transient or pika.DeliveryMode.Persistent
        username = self.environment.parsed_options.username
        password = self.environment.parsed_options.password
        self._client = RabbitMQClient(parameters=pika.URLParameters(f'amqp://{username}:{password}@{self.host}/'),
                                      exchange=self.environment.parsed_options.exchange,
                                      routing_key=self.environment.parsed_options.routing_key,
                                      delivery_mode=delivery_mode,
                                      request_event=self.environment.events.request)

    def on_stop(self):
        # Close the connection when the test is done
        self._client.disconnect()

    @task
    def send_message(self):
        self._client.publish()
