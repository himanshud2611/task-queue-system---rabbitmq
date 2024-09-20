import pika # type: ignore
import json
import time

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare task queue with dead-letter exchange (DLX)
channel.queue_declare(queue='task_queue', durable=True)

# Declare DLX and a queue for failed tasks
channel.exchange_declare(exchange='dlx', exchange_type='direct')
channel.queue_declare(queue='failed_tasks', durable=True)
channel.queue_bind(exchange='dlx', queue='failed_tasks', routing_key='failed')

def process_task(task):
    if task['task_id'] % 2 == 0:
        raise Exception("Simulated failure")
    print(f" [x] Processed {task}")

def callback(ch, method, properties, body):
    task = json.loads(body)
    try:
        process_task(task)
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge successful task
    except Exception as e:
        print(f" [x] Failed to process {task}, sending to DLX: {e}")
        # Reject and send to DLX (dead-letter exchange)
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

# Start consuming messages from 'task_queue'
channel.basic_consume(queue='task_queue', on_message_callback=callback)

print(' [*] Waiting for tasks...')
channel.start_consuming()

# log on to http://localhost:15672 to see metrices
