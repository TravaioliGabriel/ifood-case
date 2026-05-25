"""Funções auxiliares do pipeline."""

import datetime

DATE_START = "2023-01-01"
DATE_END = "2023-05-31"
S3_BASE = "s3://trip-records-taxi-data/"

SILVER_DROPNA_SUBSET = [
    "id_vendor",
    "pickup_datetime",
    "dropoff_datetime",
    "nr_passenger",
    "vl_total",
]


def cast_bronze_columns(df):
    from pyspark.sql.functions import col
    from pyspark.sql.types import DoubleType, LongType

    result = df.withColumn("VendorID", col("VendorID").cast(LongType())) \
        .withColumn("passenger_count", col("passenger_count").cast(DoubleType())) \
        .withColumn("RatecodeID", col("RatecodeID").cast(DoubleType())) \
        .withColumn("PULocationID", col("PULocationID").cast(LongType())) \
        .withColumn("DOLocationID", col("DOLocationID").cast(LongType()))

    for column in result.columns:
        if column.lower() == "airport_fee" and column != "airport_fee":
            result = result.withColumnRenamed(column, "airport_fee")

    return result


def silver_sql(taxi_type: str) -> str:
    if taxi_type == "yellow":
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
        bronze_table = "bronze.taxidata.yellow_taxi_data"
        airport_line = ",\n    airport_fee as vl_airport_fee"
    else:
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
        bronze_table = "bronze.taxidata.green_taxi_data"
        airport_line = ""

    return f"""
    select vendorID as id_vendor,
    {pickup_col} as pickup_datetime,
    {dropoff_col} as dropoff_datetime,
    passenger_count as nr_passenger,
    trip_distance as distance,
    RatecodeID as id_ratecode,
    store_and_fwd_flag as fl_store_fwd,
    PULocationID as id_pickup,
    DOLocationID as id_dropoff,
    payment_type as cd_payment,
    fare_amount as vl_fare,
    extra as vl_extra,
    mta_tax as vl_tax,
    tip_amount as vl_tip,
    tolls_amount as vl_tolls,
    improvement_surcharge as surcharge,
    total_amount as vl_total,
    congestion_surcharge as vl_surcharge_congestion{airport_line}
    from {bronze_table}
    where date({pickup_col}) between date('{DATE_START}') and date('{DATE_END}')
    and total_amount >= 0
    and {dropoff_col} >= {pickup_col}
    """


def validar_silver(spark, table: str) -> None:
    negativos = spark.sql(f"""
        SELECT
            SUM(CASE WHEN vl_total < 0 THEN 1 ELSE 0 END) as vl_total_negativo,
            SUM(CASE WHEN nr_passenger < 0 THEN 1 ELSE 0 END) as passageiros_negativo,
            SUM(CASE WHEN distance < 0 THEN 1 ELSE 0 END) as distancia_negativa
        FROM {table}
    """).collect()[0]

    assert negativos.vl_total_negativo == 0
    assert negativos.passageiros_negativo == 0
    assert negativos.distancia_negativa == 0

    periodo = spark.sql(f"""
        SELECT MIN(DATE(pickup_datetime)) as data_minima, MAX(DATE(pickup_datetime)) as data_maxima
        FROM {table}
    """).collect()[0]

    assert periodo.data_minima >= datetime.date.fromisoformat(DATE_START)
    assert periodo.data_maxima <= datetime.date.fromisoformat(DATE_END)

    nulos = spark.sql(f"""
        SELECT
            SUM(CASE WHEN id_vendor IS NULL THEN 1 ELSE 0 END) as vendor_null,
            SUM(CASE WHEN vl_total IS NULL THEN 1 ELSE 0 END) as total_null,
            SUM(CASE WHEN nr_passenger IS NULL THEN 1 ELSE 0 END) as passenger_null,
            SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) as pickup_null
        FROM {table}
    """).collect()[0]

    assert nulos.vendor_null == 0
    assert nulos.total_null == 0
    assert nulos.passenger_null == 0
    assert nulos.pickup_null == 0

    print(f"Validações OK: {table}")
