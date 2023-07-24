from sqlalchemy import create_engine
import psycopg2
from datetime import datetime
from extract import con

connection = con()


def load_data(transformed_data):
    try:
        cust_df, prod_cat_df, transactions_df = transformed_data
        try:
            # connection created with postgres db using sqlalchemy engine to create tables using DF data
            db_engine = create_engine('postgresql+psycopg2://postgres:5254@localhost:5432/postgres')

            # Creating tables and inserting data using DF into the target schema
            try:
                cust_tbl = 'customer'
                prod_cat_tbl = 'prod_cat'
                tranx_tbl = 'transactions'
                tgt_schema = 'target_schema'
                tgt_invoice_tbl = 'invoice_tbl'
                tgt_store_tbl = 'store_revenue'
                cust_df.to_sql(schema=tgt_schema, name=cust_tbl, con=db_engine, if_exists='replace', index=False)
                prod_cat_df.to_sql(schema=tgt_schema, name=prod_cat_tbl, con=db_engine, if_exists='replace', index=False)
                transactions_df.to_sql(schema=tgt_schema, name=tranx_tbl, con=db_engine, if_exists='replace', index=False)
                print("Data inserted successfully")

                with connection.cursor() as cursor:
                    # truncating the table if it has the data already to avoid data overwrite
                    cursor.execute(f"truncate table {tgt_schema}.{tgt_invoice_tbl}")

                    # inserting the data into invoice table
                    cursor.execute(f"insert into {tgt_schema}.{tgt_invoice_tbl} select c.customer_id, c.gender, "
                                   f"p.prod_cat, p.prod_subcat, t.transaction_id, cast(t.tran_date as date) , t.qty, "
                                   f"t.rate, t.tax, t.total_amt from {tgt_schema}.{cust_tbl} "
                                   f"as c left join {tgt_schema}.{tranx_tbl} as t on c.customer_id = t.cust_id "
                                   f"left join {tgt_schema}.{prod_cat_tbl} as p on t.prod_cat_code=p.prod_cat_code "
                                   f"and t.prod_subcat_code=p.prod_sub_cat_code where t.transaction_id is NOT NULL;")

                    # inserting the data into table store_revenue
                    cursor.execute(f"insert into {tgt_schema}.{tgt_store_tbl} select store_type , date_part('year', tran_date::date) as business_yr ,sum(total_amt) as revenue from "
                                   f"{tgt_schema}.{tranx_tbl} group by store_type, business_yr "
                                   f"order by store_type, business_yr  desc;")

                    connection.commit()

                # logging the task logs on success into the task_logs table
                with connection.cursor() as cursor:
                    log_tbl = 'task_logs'
                    tgt_schema = 'target_schema'
                    task_name = 'load'
                    start_date = datetime.now()
                    status = 'success'
                    description = 'Data has been loaded successfully.'
                    end_date = datetime.now()

                    cursor.execute(
                        f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
                    connection.commit()
                    print("logs inserted")

            except Exception as e:
                print(f'Error occurred: {e}')

            return "Data has been loaded successfully."
        except psycopg2.Error as e:
            print(f"Error: unable to connect to DB. \n{e}")

    except Exception as e:

        # logging the task logs on failure into the task_logs table
        with connection.cursor() as cursor:
            log_tbl = 'task_logs'
            tgt_schema = 'target_schema'
            task_name = 'load'
            start_date = datetime.now()
            status = 'failed'
            description = 'Data has not been loaded successfully.'
            end_date = datetime.now()

            cursor.execute(
                f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
            connection.commit()
            print("logs inserted")
        return f"Error: unable to load the data due to following error- \n{e}"
