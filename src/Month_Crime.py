#Month vs overall Crime Vs Count 
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext


input = sys.argv[1]
conf = SparkConf().setAppName('Crime Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#loading crime data from parquet files 
crime=sqlContext.read.parquet(input)
crime.registerTempTable('Crime')

# calculating month wise crime categories
Crime_month=sqlContext.sql('''select Month,Category,count(Category) as cnt from Crime Group by Month,Category order by Month,Category''')

# replacing month numerics with meaningful english names
Crime_month=Crime_month.na.replace(['01', '02','03','04','05','06','07','08','09','10','11','12'],
                       ['Jan', 'Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'], 'Month')

#coealescing it in a single file as it deal with final results that is small

Crime_month.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('Monwise_Crime')