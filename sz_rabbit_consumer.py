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

from senzing import G2Engine, G2Exception, G2EngineFlags, G2RetryTimeoutExceeded, G2BadInputException

INTERVAL = 10000
LONG_RECORD = 300

MSG_FRAME = 0
MSG_BODY = 2

TUPLE_MSG = 0
TUPLE_STARTTIME = 1
TUPLE_ACKED = 2

log_format = "%(asctime)s %(message)s"


def process_msg(engine, msg, info):
    try:
        record = orjson.loads(msg)
        if info:
            response = bytearray()
            engine.addRecordWithInfo(
                record["DATA_SOURCE"], record["RECORD_ID"], msg.decode(), response
            )
            return response.decode()
        else:
            engine.addRecord(record["DATA_SOURCE"], record["RECORD_ID"], msg.decode())
            return None
    except Exception as err:
        print(f"{err} [{msg}]", file=sys.stderr)
        raise


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
        "-u", "--url", dest="url", required=False, help="RabbitMQ server URL"
    )
    parser.add_argument(
        "-q", "--queue", dest="queue", required=False, help="source queue"
    )
    parser.add_argument(
        "-i",
        "--info",
        dest="info",
        action="store_true",
        default=False,
        help="produce withinfo messages",
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

    engine_config = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON")
    if not engine_config:
        print(
            "The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.",
            file=sys.stderr,
        )
        print(
            "Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API",
            file=sys.stderr,
        )
        exit(-1)

    if not args.queue:
        args.queue = os.getenv("SENZING_RABBITMQ_QUEUE")

    if not args.url:
        args.url = os.getenv("SENZING_AMQP_URL")

    max_workers = int(os.getenv("SENZING_THREADS_PER_PROCESS", 0))
    if not max_workers:  # reset to null for executors
        max_workers = None

    # Initialize the G2Engine
    g2 = G2Engine()
    g2.init("sz_rabbit_consumer", engine_config, args.debugTrace)
    logCheckTime = prevTime = time.time()

    senzing_governor = importlib.import_module("senzing_governor")
    governor = senzing_governor.Governor(hint="sz_rabbit_consumer")

    params = pika.URLParameters(args.url)
    with pika.BlockingConnection(params) as conn:
        messages = 0
        ch = conn.channel()
        ch.queue_declare(queue=args.queue, passive=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
            futures = {}
            try:
                ch.basic_qos(
                    prefetch_count=executor._max_workers
                )  # always have 1 record prefetched for each thread
                while True:

                    nowTime = time.time()
                    if futures:
                        done, _ = concurrent.futures.wait(
                            futures,
                            timeout=10,
                            return_when=concurrent.futures.FIRST_COMPLETED,
                        )

                        for fut in done:
                            msg = futures.pop(fut)
                            try:
                                result = fut.result()
                                if result:
                                    print(
                                        result
                                    )  # we would handle pushing to withinfo queues here BUT that is likely a second future task/executor
                                if not msg[
                                    TUPLE_ACKED
                                ]:  # if we rejected a message before we should not ack it here
                                    ch.basic_ack(msg[TUPLE_MSG][MSG_FRAME].delivery_tag)
                            except (G2RetryTimeoutExceeded, G2BadInputException) as err:
                                record = orjson.loads(msg[TUPLE_MSG][MSG_BODY])
                                print(
                                    f'REJECTING due to bad data or timeout: {record["DATA_SOURCE"]} : {record["RECORD_ID"]}'
                                )
                                ch.basic_reject(
                                    msg[TUPLE_MSG][MSG_FRAME].delivery_tag,
                                    requeue=False,
                                )

                            messages += 1

                            if messages % INTERVAL == 0:  # display rate stats
                                diff = nowTime - prevTime
                                speed = -1
                                if diff > 0.0:
                                    speed = int(INTERVAL / diff)
                                print(
                                    f"Processed {messages} adds, {speed} records per second"
                                )
                                prevTime = nowTime

                        if nowTime > logCheckTime + (
                            LONG_RECORD / 2
                        ):  # log long running records
                            logCheckTime = nowTime

                            response = bytearray()
                            g2.stats(response)
                            print(f"\n{response.decode()}\n")

                            numStuck = 0
                            numRejected = 0
                            for fut, msg in futures.items():
                                if not fut.done():
                                    duration = nowTime - msg[TUPLE_STARTTIME]
                                    if (
                                        duration > 2 * LONG_RECORD
                                    ):  # a record taking this long should be rejected to the dead letter queue
                                        numRejected += 1
                                        if not msg[TUPLE_ACKED]:
                                            record = orjson.loads(
                                                msg[TUPLE_MSG][MSG_BODY]
                                            )
                                            print(
                                                f'REJECTING: {record["DATA_SOURCE"]} : {record["RECORD_ID"]}'
                                            )
                                            ch.basic_reject(
                                                msg[TUPLE_MSG][MSG_FRAME].delivery_tag,
                                                requeue=False,
                                            )
                                            futures[fut] = (
                                                msg[TUPLE_MSG],
                                                msg[TUPLE_STARTTIME],
                                                True,
                                            )
                                            msg = futures[fut]
                                    if duration > LONG_RECORD:
                                        numStuck += 1
                                        record = orjson.loads(msg[TUPLE_MSG][MSG_BODY])
                                        print(
                                            f'Still processing ({duration/60:.3g} min, rejected: {msg[TUPLE_ACKED]}): {record["DATA_SOURCE"]} : {record["RECORD_ID"]}'
                                        )
                                if numStuck >= executor._max_workers:
                                    print(
                                        f"All {executor._max_workers} threads are stuck on long running records"
                                    )
                                if numRejected >= executor._max_workers:
                                    print(f"Running recovery")
                                    ch.basic_recover()  # supposedly this causes unacked messages to redeliver, should prevent the server from disconnecting us

                    # Really want something that forces an "I'm alive" to the server
                    pauseSeconds = governor.govern()
                    # either governor fully triggered or our executor is full
                    # not going to get more messages
                    if pauseSeconds < 0.0:
                        conn.sleep(1)  # process rabbitmq protocol for 1s
                        continue
                    if len(futures) >= executor._max_workers:
                        conn.sleep(1)  # process rabbitmq protocol for 1s
                        continue
                    if pauseSeconds > 0.0:
                        conn.sleep(pauseSeconds)

                    while len(futures) < executor._max_workers:
                        try:
                            msg = ch.basic_get(args.queue)
                            # print(msg)
                            if not msg[MSG_FRAME]:
                                if len(futures) == 0:
                                    conn.sleep(0.1)
                                break
                            futures[
                                executor.submit(
                                    process_msg, g2, msg[MSG_BODY], args.info
                                )
                            ] = (msg, time.time(), False)
                        except Exception as err:
                            print(f"{type(err).__name__}: {err}", file=sys.stderr)
                            raise

                print(f"Processed total of {messages} adds")

            except Exception as err:
                print(
                    f"{type(err).__name__}: Shutting down due to error: {err}",
                    file=sys.stderr,
                )
                traceback.print_exc()
                nowTime = time.time()
                for fut, msg in futures.items():
                    if not fut.done():
                        duration = nowTime - msg[TUPLE_STARTTIME]
                        record = orjson.loads(msg[TUPLE_MSG][MSG_BODY])
                        print(
                            f'Still processing ({duration/60:.3g} min, rejected: {msg[TUPLE_ACKED]}): {record["DATA_SOURCE"]} : {record["RECORD_ID"]}'
                        )
                executor.shutdown()
                conn.close()
                exit(-1)

except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc()
    exit(-1)
