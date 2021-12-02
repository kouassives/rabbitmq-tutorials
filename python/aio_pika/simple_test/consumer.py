from logging import Logger
import os
from dotenv import dotenv_values
import asyncio
import aio_pika

config = {
    **dotenv_values('.env'),  # load shared development variables
    # **dotenv_values(".env.secret"),  # load sensitive variables
    **os.environ,  # override loaded values with environment variables
}

async def main(loop):
    connection = await aio_pika.connect_robust('amqp://'+config['RABBITMQ_USERNAME']+':'+ config['RABBITMQ_PASSWORD']+'@'+config['RABBITMQ_HOST'],
    loop=loop)
    
    queue_name = "test_queue"

    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print(message.body)

                    if queue.name in message.body.decode():
                        break


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()