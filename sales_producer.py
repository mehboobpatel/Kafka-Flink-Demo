import argparse
import atexit
import json
import logging
import random
import time
import sys
from confluent_kafka import Producer

logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[
      logging.StreamHandler(sys.stdout)
  ]
)

SELLERS = ['MUM', 'PUN', 'DEL', 'GOA']

class ProducerCallback:
    def __init__(self, record, log_success=False):
        self.record = record
        self.log_success = log_success

    def __call__(self, err, msg):
        if err:
            logging.error('Error producing record {}'.format(self.record))
        elif self.log_success:
            logging.info(f"Produced: {self.record}")

def main(args):
    logging.info('Starting sales producer...')
    conf = {
        'bootstrap.servers': args.bootstrap_server,
        'linger.ms': 200,
        'client.id': 'sales-1',
    }

    producer = Producer(conf)
    atexit.register(lambda: producer.flush())

    i = 1
    while True:
        is_tenth = i % 10 == 0
        sales = {
            'seller_id': random.choice(SELLERS),
            'amount_inr': random.randint(1000, 3000),
            'sale_ts': int(time.time() * 1000)
        }

        producer.produce(args.topic, json.dumps(sales), callback=ProducerCallback(sales, is_tenth))

        if is_tenth:
            producer.poll(1)
            time.sleep(5)
            i = 0
        i += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='sales-inr')
    args = parser.parse_args()
    main(args)
