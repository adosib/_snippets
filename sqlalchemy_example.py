from sqlalchemy import create_engine, MetaData, Table, Column, select
import nzalchemy as nz

def my_create_engine(mydsn, mydatabase, **kwargs):
    connection_string = 'netezza+pyodbc://@%s' % mydsn # netezza+pyodbc://@IDP_SBX
    cargs = {'database': mydatabase}
    cargs.update(**kwargs)
    e = create_engine(connection_string, connect_args=cargs, echo=True)
    return e

engine = my_create_engine('IDP_SBX', 'IDP_PRD_US_PC_BA')
conn = engine.connect()
with conn:
    result = conn.execute("SELECT * FROM COMMON_DATA.CUST_VIEW LIMIT 100")
    for row in result:
        print(row)