import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

conf = SparkConf().setAppName("LoadStock") \
        .set("spark.shuffle.service.enabled", "false") \
        .set("spark.dynamicAllocation.enabled", "false")

####################################
# Parameters
####################################
AZURE_SQL_SERVER_NAME = sys.argv[1]
AZURE_SQL_SERVER_DATABASE = sys.argv[2]
AZURE_SQL_SERVER_USERNAME = sys.argv[3]
AZURE_SQL_SERVER_PASSWORD = sys.argv[4]
AZURE_SQL_SERVER_PORT = sys.argv[5]

AZURE_SQL_SERVER_URL = f"jdbc:postgresql://{AZURE_SQL_SERVER_NAME}/{AZURE_SQL_SERVER_DATABASE}"
####################################
# Read Stock Data
####################################

stocks_list = spark.read \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.stocks_list") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .load()

stocks_historical = spark.read \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.stocks_historical") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .load()

stocks_list.registerTempTable("stocks_list")
stocks_historical.registerTempTable("stocks_historical")

print("######################################")
print("Read Stock Data")
print("######################################")


percentage_change_close_latest = spark.sql("""
    with temp as (
        select 
            symbol,
            date,
            close,
            returns,
            rank() over (partition by symbol order by date desc) as r
        from stocks_historical
        where EXTRACT(year FROM date) = date_part('year', CURRENT_DATE)
    ), 
    returns_1st as (
        select
            symbol,
            date,
            close,
            returns
        from temp
        where r = 1 
    ), 
    returns_2nd as (
        select
            symbol,
            date,
            close
        from temp
        where r = 2
    )
    select
        r2.symbol,
        r2.date as date_2nd,
        r2.close as close_2nd,
        r1.date as date_1st,
        r1.close as close_1st,
        r1.close - r2.close as difference_change_close,
        ROUND(cast(r1.returns * 100 as numeric), 2) as percentage_change_close
    from returns_1st as r1
    inner join returns_2nd as r2
        on r1.symbol = r2.symbol
    order by r1.returns desc
""")
percentage_change_close_latest.show()
percentage_change_close_latest.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.percentage_change_close_latest") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded percentage_change_close_latest")
print("######################################")


stock_price_1st_latest = spark.sql("""
    with temp as (
        select 
            *,
            rank() over (partition by symbol order by date desc) as r
        from stocks_historical
        where EXTRACT(year FROM date) = date_part('year', CURRENT_DATE)
    )
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        market_cap,
        sma_50,
        sma_200
    from temp
    where r = 1
""")
stock_price_1st_latest.show()
stock_price_1st_latest.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.stock_price_1st_latest") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded stock_price_1st_latest")
print("######################################")


stock_price_2nd_latest = spark.sql("""
    with temp as (
        select 
            *,
            rank() over (partition by symbol order by date desc) as r
        from stocks_historical
        where EXTRACT(year FROM date) = date_part('year', CURRENT_DATE)
    )
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        market_cap,
        sma_50,
        sma_200
    from temp
    where r = 2
""")
stock_price_2nd_latest.show()
stock_price_2nd_latest.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.stock_price_2nd_latest") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded stock_price_2nd_latest")
print("######################################")


summary_stock_price_monthly = spark.sql("""
    select 
        s.symbol, 
        EXTRACT(year FROM s.date) as year,
        EXTRACT(month FROM s.date) as month,
        sum(s.market_cap) as total_market_cap,
        max(s.market_cap) as max_market_cap,
        ROUND(cast(avg(s.close) as numeric), 2) as avg_close,
        max(s.close) as max_close,
        ROUND(cast(avg(s.open) as numeric), 2) as avg_open,
        max(s.open) as max_open,
        max(s.high) as max_high,
        min(s.low) as min_low,
        max(s.volume) as max_number_of_share,
        ROUND(cast(max(s.returns) * 100 as numeric), 2) as max_change_close
    from stocks_historical  as s
    group by s.symbol, year, month
    order by s.symbol, year, month
""")
summary_stock_price_monthly.show()
summary_stock_price_monthly.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.summary_stock_price_monthly") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded summary_stock_price_monthly")
print("######################################")


summary_stock_price_yearly = spark.sql("""
    select 
        s.symbol, 
        EXTRACT(year FROM s.date) as year,
        sum(s.market_cap) as total_market_cap,
        max(s.market_cap) as max_market_cap,
        ROUND(cast(avg(s.close) as numeric), 2) as avg_close,
        max(s.close) as max_close,
        ROUND(cast(avg(s.open) as numeric), 2) as avg_open,
        max(s.open) as max_open,
        max(s.high) as max_high,
        min(s.low) as min_low,
        max(s.volume) as max_number_of_share,
        ROUND(cast(max(s.returns) * 100 as numeric), 2) as max_change_close
    from stocks_historical  as s
    group by s.symbol, year
    order by s.symbol, year
""")
summary_stock_price_yearly.show()
summary_stock_price_yearly.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.summary_stock_price_yearly") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded summary_stock_price_yearly")
print("######################################")


summary_stock_price = spark.sql("""
    select 
        symbol, 
        sum(market_cap) as total_market_cap,
        max(market_cap) as max_market_cap,
        ROUND(cast(avg(close) as numeric), 2) as avg_close,
        max(close) as max_close,
        ROUND(cast(avg(open) as numeric), 2) as avg_open,
        max(open) as max_open,
        max(high) as max_high,
        min(low) as min_low,
        max(volume) as max_number_of_share,
        ROUND(cast(max(returns) * 100 as numeric), 2) as max_change_close
    from stocks_historical 
    group by symbol
    order by total_market_cap DESC
""")
summary_stock_price.show()
summary_stock_price.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.summary_stock_price") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded summary_stock_price")
print("######################################")


total_market_cap_industry = spark.sql("""
    select 
        l.industry,
        sum(h.market_cap) as total_market_cap,
        count(distinct h.symbol) as count_symbol
    from stocks_historical as h
    inner join stocks_list as l
        on l.symbol = h.symbol
    where l.industry is not NULL and l.industry != ''
    group by l.industry
""")
total_market_cap_industry.show()
total_market_cap_industry.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", "public.total_market_cap_industry") \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded total_market_cap_industry")
print("######################################")