#Simple producer and consumer
#Demonstrates queue and event with locks

#Imports
import random
import threading
import multiprocessing
import logging
from threading import Thread
from queue import Queue
import time
import boto3
from pysqs_extended_client.SQSClientExtended import SQSClientExtended
boto3.set_stream_logger('boto3.resources', logging.ERROR)

logging.basicConfig(format='%(levelname)s - %(asctime)s.%(msecs)03d: %(message)s',datefmt='%H:%M:%S', level=logging.DEBUG)

#Functions
def display(msg):
    threadname = threading.current_thread().name
    processname = multiprocessing.current_process().name
    logging.info(f'{processname}\{threadname}: {msg}')

#Producer
def create_work(queue,finished,max):
    finished.put(False)
    count = 0
    
    sqs = boto3.client('sqs')

    while count <= 200:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )
        count = count + 1
        print('\n\n\n\n')
        print(response)
        if response.get('Messages'):
            queue.put(response.get('Messages')[0].get('Body'))

        time.sleep(0.3)
    # for x in range(max):
    #     # v = random.randint(1,100)
    #     v = x
    #     queue.put(v)
    #     display(f'Producing {x}: {v}')
    # finished.put(True)
    # display('finished')

#Consumer 
def perform_work(work,finished):
    counter = 0
    while True:
        if not work.empty():
            v = work.get()
            display(f'Consuming {counter}: {v}')
            counter += 1
            time.sleep(random.uniform(1,3))
        else:
            time.sleep(3)

        display('finished')

#Main function
def main():


    max = 5
    work = Queue()
    finished = Queue()

    producer = Thread(target=create_work,args=[work,finished,max],daemon=True)
    consumer = Thread(target=perform_work,args=[work,finished],daemon=True)
    consumer2 = Thread(target=perform_work,args=[work,finished],daemon=True)
    consumer3 = Thread(target=perform_work,args=[work,finished],daemon=True)
    consumer4 = Thread(target=perform_work,args=[work,finished],daemon=True)
    consumer5 = Thread(target=perform_work,args=[work,finished],daemon=True)
    consumer6 = Thread(target=perform_work,args=[work,finished],daemon=True)

    producer.start()
    consumer.start()
    consumer2.start()
    consumer3.start()
    consumer4.start()
    consumer5.start()
    consumer6.start()

    producer.join()
    display('Producer has finished')

    consumer.join()
    display('Consumer 1 has finished')

    consumer2.join()
    display('Consumer 2 has finished')

    consumer3.join()
    display('Consumer 3 has finished')

    consumer4.join()
    display('Consumer 4 has finished')

    consumer5.join()
    display('Consumer 5 has finished')

    consumer6.join()
    display('Consumer 6 has finished')

    # display('Finished')


if __name__ == "__main__":
    main()