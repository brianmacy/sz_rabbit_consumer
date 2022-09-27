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

from senzing import G2Engine, G2Exception, G2EngineFlags
INTERVAL=10000

MSG_FRAME = 0
MSG_BODY = 2

def process_msg(engine, msg, info):
  try:
    record = orjson.loads(msg)
    if info:
      response = bytearray()
      engine.addRecordWithInfo(record['DATA_SOURCE'],record['RECORD_ID'],msg.decode(), response)
      return response.decode()
    else:
      engine.addRecord(record['DATA_SOURCE'],record['RECORD_ID'],msg.decode())
      return None
  except Exception as err:
    print(f'{err} [{msg}]', file=sys.stderr)
    raise


try:
  parser = argparse.ArgumentParser()
  parser.add_argument('url')
  parser.add_argument('-q', '--queue', dest='queue', required=True, help='source queue')
  parser.add_argument('-i', '--info', dest='info', action='store_true', default=False, help='produce withinfo messages')
  parser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
  args = parser.parse_args()

  engine_config = os.getenv('SENZING_ENGINE_CONFIGURATION_JSON')
  if not engine_config:
    print('The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.', file=sys.stderr)
    print('Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API', file=sys.stderr)
    exit(-1)

  # Initialize the G2Engine
  g2 = G2Engine()
  g2.init("sz_rabbit_consumer",engine_config,args.debugTrace)
  prevTime = time.time()

  params = pika.URLParameters(args.url)
  with pika.BlockingConnection(params) as conn:
    messages = 0
    max_workers = None
    ch = conn.channel();
    ch.queue_declare(queue=args.queue, passive=True)
    ch.basic_qos(prefetch_count=100)

    try:
      with concurrent.futures.ThreadPoolExecutor(None) as executor:
        futures = {}
        while True:

          if futures:
            done, _ = concurrent.futures.wait(futures, timeout=10, return_when=concurrent.futures.FIRST_COMPLETED)

            for fut in done:
              result = fut.result()
              if result:
                print(result) # we would handle pushing to withinfo queues here BUT that is likely a second future task/executor
              msg = futures.pop(fut)
              ch.basic_ack(msg[MSG_FRAME].delivery_tag)
              messages+=1

              if messages%INTERVAL == 0:
                nowTime = time.time()
                speed = int(INTERVAL / (nowTime-prevTime))
                print(f'Processed {messages} adds, {speed} records per second')
                prevTime=nowTime
              if messages%100000 == 0:
                response = bytearray()
                g2.stats(response)
                print(f'\n{response.decode()}\n')

          #Really want something that forces an "I'm alive" to the server

          while len(futures) < executor._max_workers:
            try:
              msg = ch.basic_get(args.queue)
              #print(msg)
              if not msg[MSG_FRAME]:
                time.sleep(.1)
                break
              futures[executor.submit(process_msg, g2, msg[MSG_BODY], args.info)] = msg
            except Exception as err:
              print(f'{type(err).__name__}: {err}', file=sys.stderr)
              raise

      print(f'Processed total of {messages} adds')

    except Exception as err:
      print(f'{type(err).__name__}: Shutting down due to error: {err}', file=sys.stderr)
      executor.shutdown()
      conn.close()
      exit(-1)

except Exception as err:
  print(err, file=sys.stderr)
  exit(-1)	
