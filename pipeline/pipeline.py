import pandas as pd
import os
import db.database as db
import pipeline.queries as qu
from datetime import datetime

parquet_file = os.path.join( os.getcwd(), 'data', 'data.parquet.gzip')
tmp_file = os.path.join( os.getcwd(), 'data', 'data.csv')


segment_array = {
    'Frequent': qu.frequent_segment_query,
    'Recency' : qu.recency_segment_query
}

order_bracket = [
    '0-4',
    '5-13',
    '14-37'
]

days_bracket = [
    '30-60',
    '61-90',
    '91-120',
    '121-180'
]


def get_connection():
    return db.connect()


def create_tables(conn):
    status = db.execute_query(conn, qu.create_table_query)
    if status != 1:
        print("Tables Created Successfully...")
    else:
        print("Error in Table Creation...")


def extraction(conn):
    """ 
    Extraction Phase 
        * Reading data from file
        * Load it into staging table 
    """
    df = pd.read_parquet(parquet_file, engine='auto')                               # reading parquet file
    df.index = df.index + 1                                                         # starting index of df from 1 instead of 0

    df.to_csv(tmp_file, index=True, header=False)
    f = open(tmp_file, 'r')
    db.load_file(conn, f, qu.staging_table)
    f.close()
    os.remove(tmp_file)


def transformation(conn):
    """
    Transforming the staging table
        * Filter table and getting only Peru records
        * Filtering out rows with empty/null total_order and voucher_amount
        * Creating feature column 'days_since_last_order' which calculate the days since the customer last order
        * Create segement bucketing for both order and recency bracket
    """
    status = db.execute_query(conn, qu.customer_voucher_transformation_query)
    if status != 1:
        print("Transformation Completed Successfully...")
    else:
        print("Error in Transformation...")


def segment_creation(conn):
    """
    Segment Creation Phase
        *Frequent and Recency segment table created which is then used by RestAPI
    """
    for key, value in segment_array.items():
        status = db.execute_query(conn, value)
        if status != 1:
            print("{} segment created successfully...".format(key))
        else:
            print("Error in {} segment creation...".format(key))


def get_voucher(conn, voucher_request_dict):
    """ Get voucher amount for specific segment to return to API """
    if( check_segment(voucher_request_dict['segment_name'], voucher_segment_dict) ):
        response_key, response_value = voucher_segment_dict[voucher_request_dict['segment_name']](conn, voucher_request_dict)
    else:
        response_key, response_value = 'detail', 'Segment {} is not available'.format(voucher_request_dict['segment_name'])

    return { 
        response_key :  response_value
        }


def get_frequent_seqment_voucher(conn, voucher_segment_dict):
    order_bracket_value = ''

    print("Frequent Segment called...")

    for ob in order_bracket:
        if value_in_range(voucher_segment_dict['total_orders'], ob):
            order_bracket_value = ob
            break

    curr = db.execute_query(
        conn, 
        qu.voucher_amount_query.format(
            qu.frequent_segment_table,
            voucher_segment_dict['country_code'],
            'order_bracket',
            order_bracket_value
        )
    )

    curr_value = curr.fetchall()
    curr.close()

    if not curr_value:
        print('No record found')
        return 'detail', 'No Data found for given request'

    return 'voucher_amount', curr_value[0][0]


def get_recency_seqment_voucher(conn, voucher_segment_dict):
    days_bracket_value = ''

    print("Recency Segment called...")

    days_since_last_order = (datetime.now() - voucher_segment_dict['last_order_ts']).days
    
    print("Customer days since last order: {}".format(days_since_last_order))

    for _ in days_bracket:
        if days_since_last_order > 180:
            days_bracket_value = '180+'
            break
        if value_in_range(days_since_last_order, _):
            days_bracket_value = _
            break

    print("Customer Falls in bracket: {}".format(days_bracket_value))

    curr = db.execute_query(
        conn, 
        qu.voucher_amount_query.format(
            qu.recency_segment_table,
            voucher_segment_dict['country_code'],
            'recency_bracket',
            days_bracket_value
        )
    )

    curr_value = curr.fetchall()
    curr.close()
    
    if not curr_value:
        print('No record found')
        return 'detail', 'No Data found for given request'

    return 'voucher_amount', curr_value[0][0]


voucher_segment_dict = {
    'frequent_segment': get_frequent_seqment_voucher,
    'recency_segment' : get_recency_seqment_voucher
}


def check_segment(segment, segment_dict):
    return True if (segment.lower() in segment_dict) else False


def value_in_range(value, value_range):
    value_range_array = list(map(int, value_range.split('-')))
    return True if value_range_array[0] <= value <= value_range_array[1] else False


def close_connection(conn):
    db.disconnect(conn)
    print("Connection Closed.")