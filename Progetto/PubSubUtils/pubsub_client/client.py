from google.cloud import pubsub_v1
import os

class PubSubClient:
    def __init__(self, project_id, credentials_path):
        self.project_id = project_id
        os.environ['GOOGLE_APPLICATION_CREDENTIALS_PATH'] = credentials_path
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def create_topic(self, topic_name):
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        try:
            topic = self.publisher.create_topic(name=topic_path)
            print(f'Topic created: {topic.name}')
        except Exception as e:
            print(f'Error creating topic: {e}')

    def create_subscription(self, topic_name, subscription_name):
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)
        try:
            subscription = self.subscriber.create_subscription(name=subscription_path, topic=topic_path)
            print(f'Subscription created: {subscription.name}')
        except Exception as e:
            print(f'Error creating subscription: {e}')

    def publish_message(self, topic_name, message):
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        future = self.publisher.publish(topic_path, message.encode('utf-8'))
        future.result()
        print(f'Message published to {topic_name}')

    def pull_messages(self, subscription_name, timeout=10):
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)

        def callback(message):
            print(f'Received message: {message.data.decode("utf-8")}')
            message.ack()

        streaming_pull_future = self.subscriber.subscribe(subscription_path, callback=callback)
        print(f'Listening for messages on {subscription_path}...')

        try:
            streaming_pull_future.result(timeout=timeout)
        except Exception as e:
            streaming_pull_future.cancel()
            print(f'Stopping the subscriber: {e}')