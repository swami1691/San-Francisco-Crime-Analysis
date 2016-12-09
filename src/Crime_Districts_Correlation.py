# Correlation coefficient between district and crime count
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


#loading distinct district and their distinct addresses  from crime data

Crime_District=sqlContext.sql('''select distinct PdDistrict,Address
                                 from Crime
                              ''')


Crime_District.registerTempTable('Crime_District')

#assigning  number one for distinct address and also calculating crimes occured in that address and district

Crime_Add_Cnt=sqlContext.sql('''select PdDistrict,Address,'1' as one ,count(Category) as Crime_count from Crime group by PdDistrict,Address order by PdDistrict''')
Crime_Add_Cnt.registerTempTable('Crime_Address_count')


Crime_Address_count=sqlContext.sql('''select PdDistrict,sum(one) as x,sum(Crime_count) as y from Crime_Address_count where Crime_count<>'1' and PdDistrict in ('NORTHERN','SOUTHERN','CENTRAL') group by PdDistrict ''')

#finding pearson-coefficient among districts(Northern,southern and central) and their total crimes to check how correlation 
Crime_Address_count.stat.corr("x", "y")