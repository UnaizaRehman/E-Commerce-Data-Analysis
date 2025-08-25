import pandas as pd
import mysql.connector
import os
csv_files=[
    ('customers.csv','customers'),
    ("geolocation.csv",'geolocation'),
    ("order_items.csv",'order_item'),
    ('products.csv','products'),
    ("payments.csv",'payments'),
    ("orders.csv",'orders'),
    ("sellers.csv",'sellers')
]

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="pgbto2W5",
    database="Ecommerce"
)
cursor=conn.cursor()
folder_path=r'C:\Users\hp\OneDrive\Desktop\Ecommerce'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file ,table_name in csv_files:
    file_path=os.path.join(folder_path,csv_file)
    df=pd.read_csv(file_path)

    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")
    df.columns=[col.replace(' ','_').replace('-','_').replace('.','_')for col in df.columns]

    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    for _, row in df.iterrows():
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)
    conn.commit()

conn.close()

