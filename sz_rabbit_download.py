#! /usr/bin/env python3

import argparse
import logging
import traceback

import importlib
import sys
import os
import time
import random
import pika

from werkzeug.utils import secure_filename

INTERVAL = 1000

MSG_FRAME = 0
MSG_BODY = 2

log_format = "%(asctime)s %(message)s"

try:
    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)
    logging.getLogger("pika").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u", "--url", dest="url", required=True, help="RabbitMQ server URL"
    )
    parser.add_argument(
        "-q", "--queue", dest="queue", required=True, help="source queue"
    )
    parser.add_argument(
        "-o", "--output", dest="output", required=True, help="output filename to append to"
    )
    parser.add_argument(
        "-t",
        "--debugTrace",
        dest="debugTrace",
        action="store_true",
        default=False,
        help="output debug trace information",
    )
    args = parser.parse_args()

    prevTime = time.time()

    params = pika.URLParameters(args.url)
    with pika.BlockingConnection(params) as conn:
        with open(secure_filename(args.output), "a") as fpOut:
            messages = 0
            ch = conn.channel()
            ch.queue_declare(queue=args.queue, passive=True)
            for msg in ch.consume(args.queue, inactivity_timeout=1):
                if not msg or not msg[MSG_BODY]:
                    break
                try:
                    nowTime = time.time()
                    fpOut.write(msg[MSG_BODY].decode())
                    fpOut.write('\n')
                    ch.basic_ack(msg[MSG_FRAME].delivery_tag)
                    messages += 1

                    if messages % INTERVAL == 0:  # display rate stats
                        diff = nowTime - prevTime
                        speed = -1
                        if diff > 0.0:
                            speed = int(INTERVAL / diff)
                            print(
                                f"Downloaded {messages} messages, {speed} records per second"
                            )
                            prevTime = nowTime

                except Exception as err:
                    traceback.print_exc()
                    conn.close()
                    exit(-1)

            print(f"Downloaded total of {messages} messages")


except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc()
    exit(-1)
