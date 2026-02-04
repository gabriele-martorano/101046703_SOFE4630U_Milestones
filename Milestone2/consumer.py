from google.cloud import pubsub_v1      
import glob                           
import json
import os 

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

project_id = "milestones-485100";
subscription_id = "labels-sub";  

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}..\n")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = json.loads(message.data.decode('utf-8'));
    
    print(f"\nReceived record : {data}")

    for key, value in data.items():
        print(f"Value for {key} : {value}")
   
    message.ack()
    
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
