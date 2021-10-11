import os
import time
from logging import basicConfig, getLogger, INFO
from crhelper import CfnResource
from pyqldb.config.retry_config import RetryConfig
from pyqldb.driver.qldb_driver import QldbDriver

logger = getLogger(__name__)
helper = CfnResource(json_logging=False, log_level='DEBUG', boto_level='CRITICAL', sleep_on_delete=60)

try:
    retry_config = RetryConfig(retry_limit=3)
    qldb_driver = QldbDriver(os.environ['qldb_ledger'], retry_config=retry_config)
except Exception as e:
    helper.init_failure(e)

@helper.update
def update(event, context):
    try:
        logger.info("Got Update")
    except Exception as e:
        raise ValueError("CR update error: " + str(e))

@helper.delete
def delete(event, context):
    try:
        logger.info("Got Delete")
    except Exception as e:
        raise ValueError("CR delete error: " + str(e))

@helper.create  
def create(event, _):
    logger.info("Got Create")
    logger.info(event)
    try:
        qldb_driver.execute_lambda(lambda executor: create_table(executor,"Shipments"))
        time.sleep(1)
        qldb_driver.execute_lambda(lambda executor: create_index(executor,"Shipments","id"))
        time.sleep(1)
        helper.Data.update({"Response": "Success"})
    except Exception as e:
        raise ValueError("CR create error: " + str(e))
    return "Created"

def create_table(transaction_executor,tableName):
    logger.info("Creating a table")
    transaction_executor.execute_statement(f"CREATE TABLE {tableName}")

def create_index(transaction_executor,tableName,index):
    logger.info("Creating an index")
    transaction_executor.execute_statement(f"CREATE INDEX ON {tableName}({index})")

def lambda_handler(event, context):
    helper(event, context)