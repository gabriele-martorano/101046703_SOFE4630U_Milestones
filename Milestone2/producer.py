from google.cloud import pubsub_v1     
import glob                          
import json
import os 
import csv
import time

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

project_id = "milestones-485100";
topic_name = "labels";

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Published records with ordering keys to {topic_path}.")

try:
    with open("Labels.csv", mode='r') as file:
        reader = csv.DictReader(file)

        for row in reader:
            message = json.dumps(row).encode("utf-8")
            future = publisher.publish(topic_path, message)
            future.result()

    print("All records published successfully.")

except:
    print("Failed to publish the record")
    
time.sleep(.5)  
        
      
