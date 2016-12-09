#Blue Collar vs White Collar Crime  
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext

input = sys.argv[1]
conf = SparkConf().setAppName('Crime Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#loading crime data from parquet files 
crime=sqlContext.read.parquet(input).cache()
crime.registerTempTable('Crime')


Crime_Total=sqlContext.sql('''select Year,PdDistrict,count(Category) as cnt from Crime group by Year,PdDistrict''')
Crime_Total.registerTempTable('Crime_Total')


Blue_Crime=sqlContext.sql('''select Year,PdDistrict,count(Category) as blue_count from Crime 
where Category in ('VANDALISM','VEHICLE THEFT','LARCENY-THEFT','ROBBERY','BURGLARY',
'STOLEN PROPERTY','DISORDERLY CONDUCT','ASSAULT','KIDNAPPING','RECOVERED VEHICLE') group by Year,PdDistrict order by Year,blue_count desc''').cache()

Blue_Crime.registerTempTable('Blue_Crime')


White_Crime=sqlContext.sql('''select Year,PdDistrict ,count(Category) as white_count from Crime 
where Category in ('FRAUD','FORGERY-COUNTERFEITING','GAMBLING','EMBEZZLEMENT','SECONDARY CODES',
'EXTORTION','BRIBERY','BAD CHECKS','KIDNAPPING','RECOVERED VEHICLE') group by Year,PdDistrict order by Year,white_count desc''').cache()

White_Crime.registerTempTable('White_Crime')


Crime_comparision=sqlContext.sql('''select CT.Year,CT.PdDistrict,
(white_count/cnt)*100 as White_collar_crime_percent,(blue_count/cnt)*100 as Blue_collar_crime_percent
from Blue_Crime BC,White_Crime WC,Crime_Total CT
where BC.Year=WC.Year and WC.Year=CT.Year and BC.PdDistrict=WC.PdDistrict and WC.PdDistrict=CT.PdDistrict 
order by CT.Year''')


Crime_comparision.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('Blue-White-overall_percent')


Blue_Crime_Time=sqlContext.sql('''select Year,max(blue_count) as bc from Blue_Crime group by Year order by Year,bc desc ''')
Blue_Crime_Time.registerTempTable('Blue_Crime_Time')




White_Crime_Time=sqlContext.sql('''select Year,max(white_count) as wc from White_Crime group by Year order by Year,wc desc ''')
White_Crime_Time.registerTempTable('White_Crime_Time')

white_Blue_Crime=sqlContext.sql('''select WC.Year,wc,bc from White_Crime_Time WC,Blue_Crime_Time BC
where BC.Year=WC.Year''')

# coalesce is safer operation to do in this case 
white_Blue_Crime.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('Blue-White-Year_count')


