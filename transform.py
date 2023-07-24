import pandas as pd
from dateutil.parser import parse
from datetime import datetime
from extract import con

connection = con()


def transform_data(extracted_data):
    try:
        customer_df, prod_cat_df, transactions_df = extracted_data
        # converting all column names into lower case
        customer_df_cols = [col.lower().replace(" ", "") for col in customer_df.columns]
        prod_cat_df_cols = [col.lower().replace(" ", "") for col in prod_cat_df.columns]
        transactions_df_cols = [col.lower().replace(" ", "") for col in transactions_df.columns]

        # Renaming columns to lowercase
        cust_df_col_rename = {old_col: new_col for old_col, new_col in zip(customer_df.columns, customer_df_cols)}
        customer_df.rename(columns=cust_df_col_rename, inplace=True)

        prod_cat_df_col_rename = {old_col: new_col for old_col, new_col in zip(prod_cat_df.columns, prod_cat_df_cols)}
        prod_cat_df.rename(columns=prod_cat_df_col_rename, inplace=True)

        transactions_df_col_rename = {old_col: new_col for old_col, new_col in zip(transactions_df.columns, transactions_df_cols)}
        transactions_df.rename(columns=transactions_df_col_rename, inplace=True)

        # further, I'm checking that if there are any null values and then replacing it with NA
        cust_df = pd.DataFrame([customer_df[col].fillna('NA') for col in customer_df]).transpose()

        prod_cat_df = pd.DataFrame([prod_cat_df[col].fillna('NA') for col in prod_cat_df]).transpose()

        transactions_df = pd.DataFrame([transactions_df[col].fillna('NA') for col in transactions_df]).transpose()

        # Here I'll check if any quantitative data like quantity, rate, tax or total amount should not be negative
        quant_cols = ['qty', 'rate', 'tax', 'total_amt']

        for val in quant_cols:
            transactions_df[val] = transactions_df[val].abs()

        # dropping the duplicates
        cust_df = cust_df.drop_duplicates()
        prod_cat_df = prod_cat_df.drop_duplicates()
        transactions_df = transactions_df.drop_duplicates()

        # setting date format in date columns
        cust_df['dob'] = [parse(dt_str).strftime("%Y-%m-%d") for dt_str in cust_df['dob']]
        cust_df['dob'] = cust_df['dob']

        transactions_df['tran_date'] = [parse(dt_str).strftime("%Y-%m-%d") for dt_str in transactions_df['tran_date']]
        transactions_df['tran_date'] = transactions_df['tran_date']

        # logging the task logs on success into the task_logs table
        with connection.cursor() as cursor:
            log_tbl = 'task_logs'
            tgt_schema = 'target_schema'
            task_name = 'transform'
            start_date = datetime.now()
            status = 'success'
            description = 'Data has been transformed and loaded to DF successfully.'
            end_date = datetime.now()

            cursor.execute(f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
            connection.commit()
            print("logs inserted")

        return cust_df, prod_cat_df, transactions_df

    except Exception as e:
        # logging the task logs on failure into the task_logs table
        with connection.cursor() as cursor:
            log_tbl = 'task_logs'
            tgt_schema = 'target_schema'
            task_name = 'transform'
            start_date = datetime.now()
            status = 'failed'
            description = 'Data has not been transformed.'
            end_date = datetime.now()

            cursor.execute(f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
            connection.commit()
            print("logs inserted")
        return f"Error: Unable to transform the data due to following error- \n{e}"
