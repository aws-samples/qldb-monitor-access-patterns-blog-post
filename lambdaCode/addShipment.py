import os
import logging
import json
import amazon.ion.simpleion as simpleion
from pyqldb.driver.qldb_driver import QldbDriver

# Set logging
level = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
logger = logging.getLogger()
logger.setLevel(level[1])

# Set Environ variables
qldb_ledger = os.environ["QLDBLEDGER"]
qldb_table = os.environ["QLDBTABLE"]

# Set QLDB Driver
qldb_driver = QldbDriver(ledger_name=qldb_ledger)
logger.debug(f"Connected to QLDB ledger: {qldb_ledger}")

def statement_statistics(cursor):
    consumed_ios = cursor.get_consumed_ios()
    if consumed_ios:
        read_ios = consumed_ios.get('ReadIOs')
        #write_ios = consumed_ios.get('WriteIOs')
    else:
        read_ios = 0
        #write_ios = 0
    timing_information = cursor.get_timing_information()
    processing_time_milliseconds = timing_information.get('ProcessingTimeMilliseconds')
    result = {
    "processing_time_milliseconds":processing_time_milliseconds,
    "read_ios":read_ios#,
    #"write_ios":write_ios
    }
    logger.info(result)

def add_document(transaction_executor, data):
    # Check if doc with id = shipment_id exists
    statement_read = "SELECT * FROM {0} WHERE id = ?".format(qldb_table)
    logger.debug(f"Read Statement: {statement_read}")
    cursor = transaction_executor.execute_statement(statement_read, data["id"])
    statement_statistics(cursor)
    # Check if there is any record in the cursor
    logger.debug(dir(transaction_executor))
    first_record = next(cursor, None)
    if first_record:
        return_string = {
            "response": "Duplicate Item",
            "transaction_id": transaction_executor.transaction_id,
        }
    else:
        statement_insert = "INSERT INTO {0} ?".format(qldb_table)
        logger.debug(f"Insert Statement: {statement_insert}")
        cursor = transaction_executor.execute_statement(statement_insert, data)
        statement_statistics(cursor)
        # Get doc_id from inserted record
        first_record = next(cursor, None)
        return_string = {
            "response": "Processed Item",
            "transaction_id": transaction_executor.transaction_id,
            "document_id": first_record["documentId"],
        }

    return return_string


def handler(event, context):
    logger.debug(event)
    response = qldb_driver.execute_lambda(
        lambda executor: add_document(executor, json.loads(event["body"]))
    )
    logger.info(response)
    return {
        "body": "{0}".format(response),
        "headers": {"Content-Type": "json/application"},
        "statusCode": 200,
    }