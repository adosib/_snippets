import pandas as pd 
import sqlalchemy
import re
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# CLI tool that takes 2 path names for separate queries and does a diff compare check
# to identify what differences exist in the 2 queries
def execute_query(filename, conn_str):
    try:
        pd.read_sql(query, conn_str)
    except Exception as error:
        logger.exception(error)

def newlines_to_spaces(text):
    text = [line.split(",") for line in text.read().splitlines()]
    return " ".join(text)

def sanitize_sql(query: str)->str:
    """Ensures the query string does not contain the following keywords: drop, create, 
    insert, or delete.

    Args:
        query (str): the query string

    Returns:
        str: the query string if no invalid keywords were matched, else log an error msg
    """    
    # Don't want to be restrictive - just don't match a query containing drop/create/delete/insert kwds
    rgx = re.compile(r'^(?!.*drop\s)(?!.*create\s)(?!.*insert\s)(?!.*delete\s).*$', re.IGNORECASE)
    try:
        sanitized_query = re.match(rgx, query).group(0)
        return sanitized_query # may or may not be valid - don't care
    except AttributeError:
        logger.error('The sanitized query is empty. Be sure to exclude drop/create/delete/insert keywords.')

def fields_list(query: str)->list:
    """Extracts fields from a query string and returns the fields as lists.

    Args:
        query (str): the query

    Returns:
        list: a list of fields (columns) from the query
    """    
    # The regex checks for alphanumeric characters followed by whitespace and comma 
    # OR the keyword 'from' surrounded by whitespace characters
    rgx = re.compile(r'([\w`]+)(?=\s*,|\s+from\s+)', re.IGNORECASE)
    fields = re.findall(rgx, query) # string of comma-sep fields
    return fields

def query_common_fields(q1:str, q2:str):
    """Extracts the fields list from the queries and finds the intersection 
    of the two sets of field names. Wraps each query as a subquery that selects 
    only these common fields from each query.

    Args:
        q1 (str): query 1
        q2 (str): query 2

    Returns:
        tuple: a tuple of 2 elements containing the wrapped queries
    """    
    q1_fields = set(fields_list(q1))
    q2_fields = set(fields_list(q2))

    x_fields = q1_fields.intersection(q2_fields)
    q1 = f"SELECT {x_fields} FROM ({q1}) AS q1"
    q2 = f"SELECT {x_fields} FROM ({q2}) AS q2"

    return q1, q2

def process_queries(query1, query2):
    """Takes two query strings and process the text through the following steps:
    1. Removes newlines
    2. Sanitizes the text for create/update/delete operations
    3. Wraps the queries as subqueries and only selects fields the two have in common

    Args:
        query1 (str): a query string
        query2 (str): a query string

    Returns:
        tuple: a tuple of 2 elements containing the processed queries
    """    

    query1 = sanitize_sql(newlines_to_spaces(query1))
    query2 = sanitize_sql(newlines_to_spaces(query2))

    query1, query2 = query_common_fields(query1, query2)

    return query1, query2

if __name__ == "__main__":
    # TODO ask user for files or to paste in queries
    if filename_q1:
        with open(filename_q1, 'r') as q1:
            pass
    if filename_q2:
        with open(filename_q2, 'r') as q2:
            pass
    
