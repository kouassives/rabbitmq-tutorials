#!/usr/bin/env python
import asyncio
from aio_pika import connect_robust, Message

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        print(message.body)
        await asyncio.sleep(1)

async def main(loop):
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/", loop=loop
    )

    queue_name = "test_queue"
    routing_key = "test_queue"

    # Creating channel
    channel = await connection.channel()

    # Declaring exchange
    exchange = await channel.declare_exchange("direct", auto_delete=True)

    # Declaring queue
    queue = await channel.declare_queue(queue_name, auto_delete=True)

    # Binding queue
    await queue.bind(exchange, routing_key)

    await exchange.publish(
        Message(
            bytes("Hello", "utf-8"),
            content_type="text/plain",
            headers={"foo": "bar"},
        ),
        routing_key,
    )

    # Receiving message
    incoming_message = await queue.get(timeout=5)

    # Confirm message
    await incoming_message.ack()

    await queue.unbind(exchange, routing_key)
    await queue.delete()
    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(main(loop))

    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection.close())



connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        credentials=pika.PlainCredentials('guest', 'guest', erase_on_connect=True)))
channel = connection.channel()

channel.exchange_declare(exchange='chats', exchange_type='fanout')


result = channel.queue_declare(queue='', durable=True,  exclusive=False)
queue_name = result.method.queue

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
