import unittest
from unittest.mock import patch, MagicMock
from PubSubUtils.pubsub_client.client import PubSubClient


class TestPubSubClient(unittest.TestCase):

    @patch('pubsub_client.client.pubsub_v1.PublisherClient')
    @patch('pubsub_client.client.pubsub_v1.SubscriberClient')
    def setUp(self, MockSubscriberClient, MockPublisherClient):
        self.project_id = 'test-project'
        self.credentials_path = '/path/to/test-credentials.json'
        self.client = PubSubClient(self.project_id, self.credentials_path)

        self.mock_publisher = MockPublisherClient.return_value
        self.mock_subscriber = MockSubscriberClient.return_value

    def test_create_topic(self):
        topic_name = 'test-topic'
        topic_path = f'projects/{self.project_id}/topics/{topic_name}'
        self.mock_publisher.topic_path.return_value = topic_path
        self.mock_publisher.create_topic.return_value = MagicMock(name=topic_path)

        self.client.create_topic(topic_name)

        self.mock_publisher.topic_path.assert_called_once_with(self.project_id, topic_name)
        self.mock_publisher.create_topic.assert_called_once_with(name=topic_path)

    def test_create_subscription(self):
        topic_name = 'test-topic'
        subscription_name = 'test-subscription'
        topic_path = f'projects/{self.project_id}/topics/{topic_name}'
        subscription_path = f'projects/{self.project_id}/subscriptions/{subscription_name}'

        self.mock_publisher.topic_path.return_value = topic_path
        self.mock_subscriber.subscription_path.return_value = subscription_path
        self.mock_subscriber.create_subscription.return_value = MagicMock(name=subscription_path)

        self.client.create_subscription(topic_name, subscription_name)

        self.mock_publisher.topic_path.assert_called_once_with(self.project_id, topic_name)
        self.mock_subscriber.subscription_path.assert_called_once_with(self.project_id, subscription_name)
        self.mock_subscriber.create_subscription.assert_called_once_with(name=subscription_path, topic=topic_path)

    def test_publish_message(self):
        topic_name = 'test-topic'
        message = 'Test message'
        topic_path = f'projects/{self.project_id}/topics/{topic_name}'

        self.mock_publisher.topic_path.return_value = topic_path
        self.mock_publisher.publish.return_value = MagicMock()

        self.client.publish_message(topic_name, message)

        self.mock_publisher.topic_path.assert_called_once_with(self.project_id, topic_name)
        self.mock_publisher.publish.assert_called_once_with(topic_path, message.encode('utf-8'))

    def test_pull_messages(self):
        subscription_name = 'test-subscription'
        subscription_path = f'projects/{self.project_id}/subscriptions/{subscription_name}'

        self.mock_subscriber.subscription_path.return_value = subscription_path
        mock_message = MagicMock()
        mock_message.data = b'Test message'

        def callback(message):
            message.ack()

        self.mock_subscriber.subscribe.return_value = MagicMock()

        with patch('pubsub_client.client.PubSubClient.pull_messages', callback):
            self.client.pull_messages(subscription_name, timeout=1)

        self.mock_subscriber.subscription_path.assert_called_once_with(self.project_id, subscription_name)
        self.mock_subscriber.subscribe.assert_called_once_with(subscription_path, callback=callback)


if __name__ == '__main__':
    unittest.main()
