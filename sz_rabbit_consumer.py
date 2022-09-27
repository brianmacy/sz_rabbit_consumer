#! /usr/bin/env python3

import concurrent.futures

import argparse
import pathlib
import orjson
import itertools

import sys
import os
import time
import random
import pika

from queue import Empty

MSG_FRAME = 0
MSG_BODY = 2

def process_msg(msg):
  #time.sleep(random.randint(0,10))
  return msg[MSG_BODY].decode()

try:
  parser = argparse.ArgumentParser()
  parser.add_argument('url')
  parser.add_argument('-q', '--queue', dest='queue', required=True, help='source queue')
  parser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
  args = parser.parse_args()

  engine_config = os.getenv('SENZING_ENGINE_CONFIGURATION_JSON')
  if not engine_config:
    print('The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.', file=sys.stderr)
    print('Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API', file=sys.stderr)
    exit(-1)

  read = 0
  acked = 0
  params = pika.URLParameters(args.url)
  with pika.BlockingConnection(params) as conn:
    ch = conn.channel();
    ch.queue_declare(queue=args.queue, passive=True)
    ch.basic_qos(prefetch_count=100)

    try:
      with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        while True:

          if futures:
            done, _ = concurrent.futures.wait(futures, timeout=10, return_when=concurrent.futures.FIRST_COMPLETED)

            for fut in done:
              result = fut.result()
              if result:
                print(result)
              msg = futures.pop(fut)
              acked+=1
              ch.basic_ack(msg[MSG_FRAME].delivery_tag)

          #print(ch.get_waiting_message_count())

          while len(futures) < executor._max_workers:
            try:
              msg = ch.basic_get(args.queue)
              #print(msg)
              if not msg[MSG_FRAME]:
                time.sleep(.1)
                break
              read+=1
              futures[executor.submit(process_msg, msg)] = msg
            except Exception as err:
              print(f'{type(err).__name__}: {err}', file=sys.stderr)
              raise

    except Exception as err:
      print(f'{type(err).__name__}: Shutting down due to error: {err}', file=sys.stderr)
      executor.shutdown()
      conn.close()
      exit(-1)

except Exception as err:
  print(err, file=sys.stderr)
  exit(-1)	
