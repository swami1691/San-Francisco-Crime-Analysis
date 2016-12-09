import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext,HiveContext
from pyspark.sql.functions import *

inputs=sys.argv[1]
conf = SparkConf().setAppName('Month Wise Top 3 Crime')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)



crime=sqlContext.read.parquet(inputs)
crime.registerTempTable('Crime')

Crime_month=sqlContext.sql('''select  Month,Category,count(Category) as cnt
    from Crime group by Month,Category order by Month
    ''')
Crime_month=Crime_month.na.replace(['01', '02','03','04','05','06','07','08','09','10','11','12'],
                       ['Jan', 'Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'], 'Month')

Crime_month.registerTempTable('Crime_month')
#loading only aggregated records to save in csv so that hive can Query only less amount of records .
Crime_month.coalesce(1).write.format('com.databricks.spark.csv').save('MonthCategory')
# Using Hive and creating table from csv
hiveContext.sql("DROP TABLE IF EXISTS Crime_month")
hiveContext.sql("CREATE TABLE Crime_month (Month STRING, Category STRING, counts int) row format delimited fields terminated by ',' stored as textfile")
#loading csv contents into csv
hiveContext.sql("LOAD DATA INPATH '/user/chandras/MonthCategory' INTO TABLE Crime_month")

Month=hiveContext.sql("SELECT Month,Category,counts FROM (SELECT Month,Category,counts,dense_rank() OVER (PARTITION BY Month ORDER BY counts DESC) as rank FROM Crime_month order by Month) tmp WHERE rank <= 3");

#coealescing operation is safe to do  as results will not be affected 
Month.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('MonthCategory_rank')