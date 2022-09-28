#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# senzing_governor.py
#
# Class: Governor
#
# An example Senzing Governor plugin that detects transaction ID age (XID) in a
# Postgres database.  If the age is above a "high watermark",
# SENZING_GOVERNOR_POSTGRESQL_HIGH_WATERMARK, then the threads are paused
# until the age is less than a "low watermark",
# SENZING_GOVERNOR_POSTGRESQL_LOW_WATERMARK.
# Once the XID age is below the "low watermark", the threads resume processing.
#
# XID age is reduced with the Postgres vacuum command. This example doesn't
# attempt to issue the vacuum command.  The user running the Governor may not
# have the privileges to do so. When the age threshold is detected, a manual step
# of issuing the postgres vacuum command is required.
# Reference: https://www.postgresql.org/docs/current/sql-vacuum.html
#
# This example uses the native Python Postgres driver psycopg2.
# Full details on installation: https://www.psycopg.org/docs/install.html
# --------------------------------------------------------------------------------------------------------------

# Import from standard library. https://docs.python.org/3/library/

from urllib.parse import urlparse
import json
import logging
import os
import re
import string
import threading
import time

# Import from https://pypi.org/

import psycopg2

# Metadata

__all__ = []
__version__ = "1.0.6"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2020-08-26'
__updated__ = '2022-05-19'

# See https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
SENZING_PRODUCT_ID = "5017"
log_format = '%(asctime)s %(message)s'

# Lists from https://www.ietf.org/rfc/rfc1738.txt

safe_character_list = ['$', '-', '_', '.', '+', '!',
                       '*', '(', ')', ',', '"'] + list(string.ascii_letters)
unsafe_character_list = ['"', '<', '>', '#', '%',
                         '{', '}', '|', '\\', '^', '~', '[', ']', '`']
reserved_character_list = [';', ',', '/', '?', ':', '@', '=', '&']


class Governor:

    # -------------------------------------------------------------------------
    # Internal methods for database URL parsing.
    # -------------------------------------------------------------------------

    def translate(self, map, astring):
        new_string = str(astring)
        for key, value in map.items():
            new_string = new_string.replace(key, value)
        return new_string

    def get_unsafe_characters(self, astring):
        result = []
        for unsafe_character in unsafe_character_list:
            if unsafe_character in astring:
                result.append(unsafe_character)
        return result

    def get_safe_characters(self, astring):
        result = []
        for safe_character in safe_character_list:
            if safe_character not in astring:
                result.append(safe_character)
        return result

    def parse_database_url(self, original_senzing_database_url):
        ''' Given a canonical database URL, decompose into URL components. '''

        result = {}

        # Get the value of SENZING_DATABASE_URL environment variable.

        senzing_database_url = original_senzing_database_url

        # Create lists of safe and unsafe characters.

        unsafe_characters = self.get_unsafe_characters(senzing_database_url)
        safe_characters = self.get_safe_characters(senzing_database_url)

        # Detect an error condition where there are not enough safe characters.

        if len(unsafe_characters) > len(safe_characters):
            logging.error("There are not enough safe characters to do the translation. Unsafe Characters: {0}; Safe Characters: {1}".format(
                unsafe_characters, safe_characters))
            return result

        # Perform translation.
        # This makes a map of safe character mapping to unsafe characters.
        # "senzing_database_url" is modified to have only safe characters.

        translation_map = {}
        safe_characters_index = 0
        for unsafe_character in unsafe_characters:
            safe_character = safe_characters[safe_characters_index]
            safe_characters_index += 1
            translation_map[safe_character] = unsafe_character
            senzing_database_url = senzing_database_url.replace(
                unsafe_character, safe_character)

        # Parse "translated" URL.

        parsed = urlparse(senzing_database_url)
        schema = parsed.path.strip('/')

        # Construct result.

        result = {
            'dbname': self.translate(translation_map, schema),
            'host': self.translate(translation_map, parsed.hostname),
            'password': self.translate(translation_map, parsed.password),
            'port': self.translate(translation_map, parsed.port),
            'user': self.translate(translation_map, parsed.username),
        }

        # Return result.

        return result

    def parse_string(self, format_string, string_to_be_parsed):
        """
        Match string_to_be_parsed against the given format string, return dictionary of matches.
        See https://stackoverflow.com/questions/10663093/use-python-format-string-in-reverse-for-parsing
        """

        # First split on any keyword arguments, note that the names of keyword arguments will be in the
        # 1st, 3rd, ... positions in this list

        tokens = re.split(r'\{(.*?)\}', format_string)
        keywords = tokens[1::2]

        # Now replace keyword arguments with named groups matching them. We also escape between keyword
        # arguments so we support meta-characters there. Re-join tokens to form our regexp pattern

        tokens[1::2] = map(u'(?P<{}>.*)'.format, keywords)
        tokens[0::2] = map(re.escape, tokens[0::2])
        pattern = ''.join(tokens)

        # Use our pattern to match the given string, raise if it doesn't match

        matches = re.match(pattern, string_to_be_parsed)
        if not matches:
            raise Exception("Format string did not match")

        # Return a dict with all of our keywords and their values

        return {x: matches.group(x) for x in keywords}

    # -------------------------------------------------------------------------
    # Internal methods for extracting
    # -------------------------------------------------------------------------

    def extract_database_urls(self, config_json, default):
        if config_json:
            config_dict = json.loads(config_json)
            database_urls = [config_dict["SQL"]["CONNECTION"]]
            hybrid = config_dict.get('HYBRID', {})
            database_keys = set(hybrid.values())
            for database_key in database_keys:
                database = config_dict.get(database_key, {}).get("DB_1", None)
                if database:
                    database_urls.append(database)

            # Transform Database URLs.

            postgresql_url_input_template = "{scheme}://{username}:{password}@{hostname}:{port}:{schema}"
            postgresql_url_output_template = "{scheme}://{username}:{password}@{hostname}:{port}/{schema}"
            result_list = []
            for database_url in database_urls:
                parsed_database_url = self.parse_string(
                    postgresql_url_input_template, database_url)
                result_list.append(
                    postgresql_url_output_template.format(**parsed_database_url))

            result = ','.join(result_list)
        else:
            result = default
        return result

    # -------------------------------------------------------------------------
    # Internal methods for accessing database.
    # -------------------------------------------------------------------------

    def get_current_watermark(self, cursor):

        cursor.execute(self.sql_stmt)
        result = cursor.fetchone()
        return result[0], result[1]

    # -------------------------------------------------------------------------
    # Support for Python Context Manager.
    # -------------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    # -------------------------------------------------------------------------
    # Public API methods.
    #  - govern()
    #  - close()
    # -------------------------------------------------------------------------

    def __init__(
        self,
        database_urls=None,
        high_watermark=1_500_000_000,
        hint="",
        interval=100_000,
        list_separator=',',
        low_watermark=1_200_000_000,
        log_interval_in_seconds=600,
        check_time_interval_in_seconds=5,
        *args,
        **kwargs
    ):

        logging.info("senzing-{0}0001I Using governor-postgresql-transaction-id Governor. Version: {1} Updated: {2}".format(
            SENZING_PRODUCT_ID, __version__, __updated__))

        # Instance variables. Precedence: 1) OS Environment variables, 2) parameters

        self.old_wait_time = 0
        self.counter = 0
        self.counter_lock = threading.Lock()
        self.last_log_time = 0
        # update this data structure to change the back-off step times.
        #  1.0 means that we're at the highwater mark so we should pause longer
        #  to allow the database to catch up.  More steps could be added or times
        #  modified to fit required performance characteristics.
        # <ratio>:<time in seconds>
        self.step_ratios = [
            (1.0, -1),
            (0.8, 100),
            (0.4, 10),
            (0.2, 1),
            (0.1, 0.1),
            (0.0, 0.01),
        ]

        # Database connection string. Precedence: 1) SENZING_GOVERNOR_DATABASE_URLS, 2) SENZING_DATABASE_URL, 3) SENZING_ENGINE_CONFIGURATION_JSON 4) parameters

        self.database_urls = database_urls
        self.database_urls = self.extract_database_urls(
            os.getenv("SENZING_ENGINE_CONFIGURATION_JSON"), self.database_urls)
        self.database_urls = os.getenv(
            "SENZING_DATABASE_URL", self.database_urls)
        self.database_urls = os.getenv(
            "SENZING_GOVERNOR_DATABASE_URLS", self.database_urls)

        self.high_watermark = int(
            os.getenv("SENZING_GOVERNOR_POSTGRESQL_HIGH_WATERMARK", high_watermark))
        self.hint = os.getenv("SENZING_GOVERNOR_HINT", hint)
        self.interval = int(os.getenv("SENZING_GOVERNOR_INTERVAL", interval))
        self.list_separator = os.getenv(
            "SENZING_GOVERNOR_LIST_SEPARATOR", list_separator)
        self.low_watermark = int(
            os.getenv("SENZING_GOVERNOR_POSTGRESQL_LOW_WATERMARK", low_watermark))
        self.sql_stmt = "SELECT c.oid::regclass, age(c.relfrozenxid), pg_size_pretty(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid WHERE relkind IN ('r', 't', 'm') AND n.nspname NOT IN ('pg_toast') ORDER BY 2 DESC LIMIT 1;"
        self.check_time_interval_in_seconds = int(os.getenv(
            "SENZING_GOVERNOR_CHECK_TIME_INTERVAL_IN_SECONDS", check_time_interval_in_seconds))
        self.log_interval_in_seconds = int(
            os.getenv("SENZING_GOVERNOR_LOG_INTERVAL_IN_SECONDS", log_interval_in_seconds))
        logging.info("senzing-{0}0002I SENZING_GOVERNOR_POSTGRESQL_HIGH_WATERMARK: {1}; SENZING_GOVERNOR_INTERVAL: {2}; SENZING_GOVERNOR_POSTGRESQL_LOW_WATERMARK {3}; SENZING_GOVERNOR_HINT: {4}; SENZING_GOVERNOR_LOG_INTERVAL_IN_SECONDS: {5}".format(
            SENZING_PRODUCT_ID, self.high_watermark, self.interval, self.low_watermark, self.hint, self.log_interval_in_seconds))

        # Synthesize variables.

        self.next_check_time = time.time() + self.check_time_interval_in_seconds

        # Make database connections.

        self.database_connections = {}
        if self.database_urls:
            sql_connection_strings = self.database_urls.split(
                self.list_separator)
            for database_connection_string in sql_connection_strings:

                parsed_database_url = self.parse_database_url(
                    database_connection_string)

                # Only "postgresql://" databases are monitored.

                schema = database_connection_string.split(':')[0]
                if schema != "postgresql":
                    logging.info("senzing-{product_id}0701W SENZING_GOVERNOR_DATABASE_URLS contains a non-postgres URL:  {schema}://{user}:xxxxxxxx@{host}:{port}/{dbname}".format(
                        product_id=SENZING_PRODUCT_ID, schema=schema, **parsed_database_url))
                    continue

                # Create connection.

                logging.info("senzing-{product_id}0003I Governor monitoring {schema}://{user}:xxxxxxxx@{host}:{port}/{dbname}".format(
                    product_id=SENZING_PRODUCT_ID, schema=schema, **parsed_database_url))
                connection = psycopg2.connect(**parsed_database_url)
                connection.set_session(
                    autocommit=True, isolation_level='READ UNCOMMITTED', readonly=True)
                cursor = connection.cursor()

                self.database_connections[database_connection_string] = {
                    'parsed_database_url': parsed_database_url,
                    'connection': connection,
                    'cursor': cursor,
                }

    def get_wait_time(self, watermark):
        """
        There are several strategies one could use for determining wait time.
        We are implementing a static step function.  Another option is to
        use an exponential (or linear) backoff strategy.  Yet another option is
        to use a combination of the two where you smoothly ramp up wait time
        between stepped ranges.
        """

        watermark_delta = self.high_watermark - self.low_watermark
        watermark_ratio = (watermark - self.low_watermark) / watermark_delta

        # an example of an exponential backoff strategy would be:
        # watermark_percentage = watermark_ratio * 100
        # wait_time = ((1.1**watermark_percentage)-1)/100

        for step in self.step_ratios:
            if watermark_ratio > step[0]:
                return step[1]

        return 0

    def govern(self, *args, **kwargs):
        """
        Do the actual "governing".
        Do not return until the governance has been completed.
        The caller of govern() waits synchronously.
        """

        # counter_lock serializes threads.

        with self.counter_lock:
            self.counter += 1

            # Only make expensive checks after "interval" records have been read.

            if (self.counter % self.interval == 0) or (time.time() > self.next_check_time):

                # Reset timer.

                self.next_check_time = time.time() + self.check_time_interval_in_seconds

                # Go through each database connection to determine if watermark is above high_watermark.

                for database_connection in self.database_connections.values():
                    cursor = database_connection.get("cursor")
                    database_host = database_connection.get(
                        "parsed_database_url", {}).get("host")
                    database_name = database_connection.get(
                        "parsed_database_url", {}).get("dbname")
                    oid_name, watermark = self.get_current_watermark(cursor)

                    current_log_time = time.time()
                    # only log a message when the log interval has passed
                    if ((current_log_time - self.last_log_time) > self.log_interval_in_seconds):
                        logging.info("senzing-{0}0004I Governor is checking PostgreSQL Transaction IDs. Host: {1}; Database: {2}; Current XID: {3} ({4}); High watermark XID: {5}".format(
                            SENZING_PRODUCT_ID, database_host, database_name, watermark, oid_name, self.high_watermark))
                        self.last_log_time = current_log_time

                    # When we get above the low water mark, use our wait time
                    # function to start to slow down.

                    if watermark > self.low_watermark: # This all needs to be done based on the worst XID if all DBs
                        wait_time = self.get_wait_time(watermark)
                        current_log_time = time.time()
                        # log a message when the wait_time changes OR if the log interval has passed
                        if (wait_time != self.old_wait_time) or ((current_log_time - self.last_log_time) > self.log_interval_in_seconds):
                            logging.info("senzing-{0}0005I Governor waiting {1} seconds for {2} database age(XID) to go from current value of {3} ({4}) to low watermark of {5}.".format(
                                SENZING_PRODUCT_ID, wait_time, database_name, watermark, oid_name, self.low_watermark))
                            self.old_wait_time = wait_time
                            self.last_log_time = current_log_time
                    elif self.old_wait_time != 0:
                        logging.info("Governor delay ended. Returning to no wait.")
                        self.old_wait_time = 0
        return self.old_wait_time

    def close(self, *args, **kwargs):
        '''  Tasks to perform when shutting down, e.g., close DB connections '''

        for database_connection in self.database_connections.values():
            database_connection.get('cursor').close()
            database_connection.get('connection').close()
        logging.info(
            "senzing-{0}0006I Governor closed.".format(SENZING_PRODUCT_ID))


if __name__ == '__main__':

    # Configure logging. See https://docs.python.org/2/library/logging.html#levels

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

    # Create governor.

    governor = Governor()

    # Print databases specified by environment variables.

    sql_connection_strings = governor.database_urls.split(
        governor.list_separator)
    for sql_connection_string in sql_connection_strings:
        logging.info(
            "senzing-{0}0007I Database: {1}".format(SENZING_PRODUCT_ID, sql_connection_string))

