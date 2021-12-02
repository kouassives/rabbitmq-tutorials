#!/usr/bin/env python
import pika, sys, os

def main():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        credentials=pika.PlainCredentials('guest', 'guest', erase_on_connect=True)))
    channel = connection.channel()

    channel.exchange_declare(exchange='lacrateExchange', exchange_type='topic')

    channel.queue_declare(queue='frontend_topic_{ObjectUUID}', exclusive=True)

    channel.queue_bind(exchange='lacrateExchange', queue='frontend_topic_ObjectUUID')

    def callback(ch, method, properties, body):
        print(" [x] %r" % body.decode())

    print(' [*] Waiting for logs. To exit press CTRL+C')
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


























#!/usr/bin/env python
import threading
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        credentials=pika.PlainCredentials('guest', 'guest', erase_on_connect=True)))
channel = connection.channel()

channel.exchange_declare(exchange='lacarte', exchange_type='fanout')


result = channel.queue_declare(queue='backend', durable=True,  exclusive=False)
queue_name = 'user1' #result.method.queue

channel.queue_bind(exchange='chats', queue=queue_name)

def callback(ch, method, properties, body):
    print(" [x] %r" % body.decode())

print(' [*] Waiting for logs. To exit press CTRL+C')
channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

threading.Thread(target=channel.start_consuming).start()

msg = input('Type your msg:\n')
while input :
    channel.basic_publish(exchange='chats', routing_key='', body=msg)
    # print(" [v] Sent %r" % msg)
    msg = input('')

connection.close()
