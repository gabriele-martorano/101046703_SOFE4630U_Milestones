from google.cloud import pubsub_v1
import glob
import json
import os

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "milestones-485100"

input_subscription = "smartMeter-sub"
output_topic = "smartMeterFiltered"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subscription_path = subscriber.subscription_path(project_id, input_subscription)
topic_path = publisher.topic_path(project_id, output_topic)

print(f"Listening for messages on {subscription_path}..\n")

def callback(message):

    message_data = json.loads(message.data.decode('utf-8'))

    # FILTER: eliminate records containing None
    if (message_data["temperature"] is not None and
        message_data["humidity"] is not None and
        message_data["pressure"] is not None):

        record_value = json.dumps(message_data).encode('utf-8')
        publisher.publish(topic_path, record_value)
        print("Filtered and published:", message_data)

    message.ack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
