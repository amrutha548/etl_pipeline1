from kafka import KafkaConsumer
import boto3
import json

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'news_topic',  # Topic name to consume messages from
    bootstrap_servers='localhost:9092',  # Replace with your Kafka server address
    auto_offset_reset='earliest',  # Start from the beginning of the topic if no offset is committed
    enable_auto_commit=True,  # Automatically commit the offset after consuming the message
    group_id='your_group_id',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the JSON data
)

# Initialize S3 Client with the correct region
s3 = boto3.client('s3', region_name='ap-south-1')
bucket_name = 'newsapi-s3'

print("Kafka consumer is ready and listening for messages...")

# Read messages from the Kafka topic and process them
for message in consumer:
    article = message.value
    print(f"Received article from Kafka: {article.get('title')}")

    try:
        # Debugging: Print the data that will be uploaded to S3
        print(f"Uploading article to S3: {json.dumps(article)}")

        # Upload the message to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=f"your_prefix/{message.offset}.json",  # Use message offset as the S3 object key
            Body=json.dumps(article)  # Upload the JSON data
        )
        print(f"Article uploaded to S3 with key: your_prefix/{message.offset}.json")
    
    except Exception as e:
        print(f"Error uploading to S3: {e}")


