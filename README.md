# sz_rabbit_consumer

# Overview
Simple parallel JSON data processor using the Senzing API and is meant to provide developers with a simple starting point for a simple, scalable addRecord processor.

It is a limited function [more or less] drop in replacement for the senzing/stream-loader when used for RabbitMQ.  The latest code requires Senzing 3.5 or newer.

# API demonstrated
### Core
* addRecord[WithInfo]: Adds the Senzing JSON record
### Supporting
* init: To initialize the G2Engine object
* stats: To retrieve internal engine diagnostic information as to what is going on in the engine

For more details on the Senzing API go to https://docs.senzing.com

# Details

### Required parameter (environment)
```
SENZING_ENGINE_CONFIGURATION_JSON
SENZING_RABBITMQ_QUEUE
SENZING_AMQP_URL
```

### Optional parameters (environment)
```
SENZING_LOG_LEVEL (default: info)
SENZING_THREADS_PER_PROCESS (default: based on whatever concurrent.futures.ThreadPoolExecutor chooses automatically)
LONG_RECORD: (default: 300 seconds)
```

## Building/Running
```
docker build -t brian/sz_rabbit_consumer .
docker run --user $UID -it -e SENZING_ENGINE_CONFIGURATION_JSON -e SENZING_RABBITMQ_QUEUE -e SENZING_AMQP_URL brian/sz_rabbit_consumer
```

## Additional items to note
 * Will exit on non-data related exceptions after processing or failing to process the current records in flight
 * If a record takes more than 5min to process (LONG_RECORD), it will let you know which record it is and how long it has been processing
 * Does not use the senzing-###### format for log messages (unlike the senzing/stream-loader) and simply uses python `print` with strings.  It does use the standard senzing governor-postgresql-transaction-id module so you will see some messages using the standard format.
 * Does not support "WithInfo" output to queues but you can provide a "-i" command line option that will enable printing the WithInfo responses out.  It is simple enough to code in whatever you want done with WithInfo messages in your solution.
