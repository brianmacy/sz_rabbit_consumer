#! /usr/bin/env python3

import concurrent.futures

import argparse
import orjson
import logging
import traceback

import importlib
import sys
import os
import time
import random
import pika

from senzing import G2Engine, G2Exception, G2EngineFlags
INTERVAL=10000

MSG_FRAME = 0
MSG_BODY = 2

log_format = '%(asctime)s %(message)s'

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
  log_level_map = {
      "notset": logging.NOTSET,
      "debug": logging.DEBUG,
      "info": logging.INFO,
      "fatal": logging.FATAL,
      "warning": logging.WARNING,
      "error": logging.ERROR,
      "critical": logging.CRITICAL
  }

  log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
  log_level = log_level_map.get(log_level_parameter, logging.INFO)
  logging.basicConfig(format=log_format, level=log_level)
  logging.getLogger("pika").setLevel(logging.WARNING)


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

  senzing_governor = importlib.import_module("senzing_governor")
  governor = senzing_governor.Governor(hint="sz_rabbit_consumer")

  params = pika.URLParameters(args.url)
  with pika.BlockingConnection(params) as conn:
    messages = 0
    max_workers = None
    threads_per_process = os.getenv('SENZING_THREADS_PER_PROCESS', None)
    if threads_per_process:
      max_workers = int(threads_per_process)
    ch = conn.channel();
    ch.queue_declare(queue=args.queue, passive=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
      futures = {}
      try:
        ch.basic_qos(prefetch_count=executor._max_workers) # always have 1 record prefetched for each thread
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
          pauseSeconds = governor.govern()
          # either governor fully triggered or our executor is full
          # not going to get more messages
          if pauseSeconds < 0:
            conn.process_data_events(1) # process rabbitmq protocol for 1s
            continue
          if len(futures) >= executor._max_workers:
            conn.process_data_events(0) # process rabbitmq protocol just once
            continue

          while len(futures) < executor._max_workers:
            try:
              if pauseSeconds > 0:
                conn.sleep(pauseSeconds)
              msg = ch.basic_get(args.queue)
              #print(msg)
              if not msg[MSG_FRAME] and not len(futures):
                conn.sleep(.1)
                break
              futures[executor.submit(process_msg, g2, msg[MSG_BODY], args.info)] = msg
            except Exception as err:
              print(f'{type(err).__name__}: {err}', file=sys.stderr)
              raise

        print(f'Processed total of {messages} adds')

      except Exception as err:
        print(f'{type(err).__name__}: Shutting down due to error: {err}', file=sys.stderr)
        traceback.print_exc()
        for fut, msg in futures.items():
          if not fut.done():
            record = orjson.loads(msg[MSG_BODY])
            print(f'Still processing: {record["DATA_SOURCE"]} : {record["RECORD_ID"]}')
        executor.shutdown()
        conn.close()
        exit(-1)

except Exception as err:
  print(err, file=sys.stderr)
  traceback.print_exc()
  exit(-1)

