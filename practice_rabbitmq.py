import pika
import threading
import json
from time import sleep
from datetime import datetime


def message():
    a = {'name': 'Andy',
         'age': '23',
         'city': 'NY'}
    return json.dumps(a)


def send(queue_name):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    with connection as c:
        channel = c.channel()
        channel.queue_declare(queue=queue_name)
        body = message()
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=body)
        print("Sent Message to '{}'".format(queue_name))
    pass


def process_data(sleep_interval):
    for i in range(sleep_interval):
        print('doing work {} out of {}'.format(i + 1, sleep_interval))
        send('process_queue')
        sleep(1)
    pass


def init_rabbitmq(queue_name, connection_name):
    props = {'connection_name': connection_name}
    parameters = pika.ConnectionParameters('localhost', client_properties=props)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel


def consumer(ch, queue_name):
    ch.basic_consume(queue=queue_name,
                     on_message_callback=my_callback,
                     auto_ack=False)
    print('Waiting for messages on: {}'.format(queue_name))
    ch.start_consuming()


def my_callback(ch, method, properties, body):
    body_json = json.loads(body)
    print('listener received message\nstart new process thread: ', datetime.now())
    process_thread = threading.Thread(target=process_data, args=(240,))
    process_thread.start()
    while process_thread.is_alive():
        ch._connection.sleep(1.0)
    print('process thread finished, ', datetime.now())
    ch.basic_ack(delivery_tag=method.delivery_tag)
    pass


def main():
    queue_name = 'test'
    send_thread = threading.Thread(target=send,
                                   args=(queue_name,),
                                   daemon=False)
    send_thread.start()

    main_channel = init_rabbitmq(queue_name, 'main_listener')
    consumer(main_channel, queue_name)
    pass


if __name__ == '__main__':
    main()
    pass
