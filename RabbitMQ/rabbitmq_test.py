import pika # type: ignore

#Producer sending a  message

# establishing connection to rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

# declaring a queue
channel.queue_declare(queue = "payment")

#sending a message to queue
channel.basic_publish(exchange = '', routing_key = "payment", body = "processing payment for order")

print("[x] sent 'processing payment'")

#closing connection
connection.close

#Consumer receiving a message
# establishing connection to rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

# declaring a queue
channel.queue_declare(queue = "payment")

# callback function to handle message
def callback(ch, method, props, body):
    print(f" [x] received {body}")

# telling rabitmq that this function should receive messages from the 'payment' queue
channel.basic_consume(queue='payment', on_message_callback = callback, auto_ack= True)

print('[*] waiting for messages')
channel.start_consuming()
