# Producer will create tasks and send them to RabbitMQ. 
# Each task could be a simple string or a complex object serialized to JSON

import pika # type: ignore
import json
import sys

# Establish connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a durable queue -- queue and messages are persisted even if RabbitMQ restarts
channel.queue_declare(queue='task_queue', durable=True)

def publish_task(task_data):
    # Serialize task to JSON format (example task)
    task_body = json.dumps(task_data)

    # Publish message to the queue
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=task_body,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent -- RabbitMQ will save them to disk to prevent data loss
        )
    )
    print(f" [x] Sent {task_data}")

# Example tasks
tasks = [{'task_id': i, 'data': f'Task data {i}'} for i in range(5)]

# Send each task
for task in tasks:
    publish_task(task)

# Close connection
connection.close()
