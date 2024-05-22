from pubsub_client.client import PubSubClient
import config


def main():
    client = PubSubClient(config.PROJECT_ID, config.CREDENTIALS_PATH)

    client.create_topic(config.TOPIC_NAME)
    client.create_subscription(config.TOPIC_NAME, config.SUBSCRIPTION_NAME)

    client.publish_message(config.TOPIC_NAME, 'Ciao, mondo!')

    client.pull_messages(config.SUBSCRIPTION_NAME, timeout=10)


if __name__ == '__main__':
    main()
