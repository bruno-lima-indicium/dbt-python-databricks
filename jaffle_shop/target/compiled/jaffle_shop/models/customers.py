import pyspark.sql.functions as F

def model(dbt, session):

    dbt.config(
        submission_method="all_purpose_cluster",
        create_notebook=False,
        cluster_id="0124-220040-qfvx2ysq"
    )

    stg_customers_df = dbt.ref('stg_customers')
    stg_orders_df = dbt.ref('stg_orders')
    stg_payments_df = dbt.ref('stg_payments')

    customer_orders_df = (
        stg_orders_df
        .groupby("customer_id")
        .agg(
            F.min(F.col("order_date")).alias('first_order'),
            F.max(F.col("order_date")).alias('most_recent_order'),
            F.count(F.col("order_id")).alias('number_of_orders')
        )
    )

    customer_payments_df = (
        stg_payments_df
        .join(stg_orders_df, stg_payments_df.order_id == stg_orders_df.order_id, "left")
        .groupby(stg_orders_df.customer_id)
        .agg(
            F.sum(F.col("amount")).alias('total_amount')
        )
    )

    final_df = (
        stg_customers_df.alias("customers") \
            .join(customer_orders_df.alias("customer_orders"), F.col("customers.customer_id") == F.col("customer_orders.customer_id"), "left") \
            .join(customer_payments_df.alias("customer_payments"), F.col("customers.customer_id") == F.col("customer_payments.customer_id"), "left") \
            .select(F.col("customers.customer_id").alias("customer_id"),
                    F.col("customers.first_name").alias("first_name"),
                    F.col("customers.last_name").alias("last_name"),
                    F.col("customer_orders.first_order").alias("first_order"),
                    F.col("customer_orders.most_recent_order").alias("most_recent_order"),
                    F.col("customer_orders.number_of_orders").alias("number_of_orders"),
                    F.col("customer_payments.total_amount").alias("customer_lifetime_value")
            )
    )

    return final_df



# stg_customers_df = spark.table("stg_customers")
# stg_orders_df = spark.table("stg_orders")
# stg_payments_df = spark.table("stg_payments")

# # customer_orders
# customer_orders_df = stg_orders_df.groupBy("customer_id") \
#                                   .agg(min("order_date").alias("first_order"), 
#                                        max("order_date").alias("most_recent_order"), 
#                                        count("order_id").alias("number_of_orders"))

# # customer_payments
# customer_payments_df = stg_payments_df.join(stg_orders_df, stg_payments_df["order_id"] == stg_orders_df["order_id"], "left") \
#                                       .groupBy("customer_id") \
#                                       .agg(sum("amount").alias("total_amount"))

# # final
# final_df = stg_customers_df.join(customer_orders_df, stg_customers_df["customer_id"] == customer_orders_df["customer_id"], "left") \
#                            .join(customer_payments_df, stg_customers_df["customer_id"] == customer_payments_df["customer_id"], "left") \
#                            .selectExpr("customer_id", "first_name", "last_name", "first_order", "most_recent_order", "number_of_orders", "total_amount as customer_lifetime_value")

# final_df.show()


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args,dbt_load_df_function):
    refs = {"stg_customers": "bruno.stg_customers", "stg_orders": "bruno.stg_orders", "stg_payments": "bruno.stg_payments"}
    key = ".".join(args)
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = ".".join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = 'None'
    schema = 'bruno'
    identifier = 'customers'
    def __repr__(self):
        return 'bruno.customers'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------

# how to execute python model in notebook
# dbt = dbtObj(spark.table)
# df = model(dbt, spark)

