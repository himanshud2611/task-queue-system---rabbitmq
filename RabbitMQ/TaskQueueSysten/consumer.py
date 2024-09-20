# listen messages from RabbitMQ, Process each task and then acknowledge completion

import pika # type: ignore
import json
import time

def process_task(body):
    print(f" [x] Processed {body}")

def callback(ch, method, properties, body):
    # Deserialize the task from JSON
    task = json.loads(body)
    
    print(f" [x] Received {task}")
    
    # Simulate task processing (e.g., CPU-bound computation or I/O-bound operations)
    time.sleep(5)  # Simulating a delay in processing
    print(f" [x] Processed {task}")
    
    # Acknowledge the task as complete
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Establish connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=600))
channel = connection.channel()

# Declare queue
channel.queue_declare(queue='task_queue', durable=True)

# Callback to process the task
def callback(ch, method, properties, body):
    task = json.loads(body)
    print(f" [x] Received {task}")
    # Simulate task processing (e.g., CPU-bound computation or I/O-bound operations)
    time.sleep(5)  # Simulating a delay in processing
    print(f" [x] Processed {task}")
    # Acknowledge message after processing
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Consuming messages from task_queue
channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=False)

try:
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except KeyboardInterrupt:
    print('Interrupted')
    channel.stop_consuming()
finally:
    connection.close()

