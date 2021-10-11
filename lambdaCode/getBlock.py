import os
import json
import boto3
import logging
import amazon.ion.simpleion as ion
from amazon.ion.json_encoder import IonToJSONEncoder
from pyqldb.driver.qldb_driver import QldbDriver


# Set QLDB client
qldb_client = boto3.client('qldb')

# Set logging
level = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
logger = logging.getLogger()
logger.setLevel(level[1])

# Set Environ variables
qldb_ledger = os.environ["QLDBLEDGER"]
qldb_table = os.environ["QLDBTABLE"]

# Set QLDB Driver
qldb_driver = QldbDriver(ledger_name=qldb_ledger)

# get block info from history function with document id and transaction id
def get_block_info(transaction_executor, data):

    # Check if doc with GovId = gov_id exists
    statement = "SELECT blockAddress FROM history({0}) WHERE metadata.id = ? and metadata.txId = ?".format(qldb_table)
    
    cursor = transaction_executor.execute_statement(statement, data['document_id'], data['transaction_id'])
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    logger.debug(json.dumps(first_record, cls=IonToJSONEncoder))
    block_info = first_record
    
    return block_info
    
# get block from QLDB journal
def get_block(blockaddress):
    blockAddress_clean = json.dumps(blockaddress, cls=IonToJSONEncoder)
    response = qldb_client.get_block(
        Name=qldb_ledger,
        BlockAddress={
            'IonText': blockAddress_clean 
        }
    )
    return response

# get statements from ion text
def get_statements(statements):
    json_statements = []
    for each in statements:
        each.pop('statementDigest')
        json_statements.append(json.dumps(each, cls=IonToJSONEncoder))
        
    return json_statements

def handler(event, context):
    logger.debug(event)
    block_info = qldb_driver.execute_lambda(lambda executor: get_block_info(executor, json.loads(event['body'])))
    result = get_block(block_info['blockAddress'])
    # convert to ion
    result_ion = ion.loads(result['Block']['IonText'])
    logger.debug(json.dumps(result_ion, cls=IonToJSONEncoder))
    # return only the transaction statements
    if json.loads(event['body'])['response'] == 'full':
        txn_statements = json.dumps(result_ion, cls=IonToJSONEncoder)
    elif json.loads(event['body'])['response'] == 'statement_summary':
        txn_statements = get_statements(result_ion['transactionInfo']['statements'])
    else:
        # add to for more options
        txn_statements = get_statements(result_ion['transactionInfo']['statements'])
    return {
        "body": '"{0}"'.format(txn_statements),  
        "headers": {
        "Content-Type": "application/json"
        },
        "statusCode": 200
    }