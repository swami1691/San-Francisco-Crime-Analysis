#District Wise Resolution closed Count 
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

# taking count of resolution against the crime excluding resolution in Pending/In-progress/No Resolution
Crime_Resolution=sqlContext.sql('''select PdDistrict,count(Resolution) as cnt from Crime Where Resolution<>'NONE'  Group by PdDistrict order by PdDistrict''')
Crime_Resolution.registerTempTable('Crime_Resolution')

# ignoring bad data
Crime_Res=sqlContext.sql('''select * from Crime_Resolution where cnt<>1 ''').cache()
Crime_Res.registerTempTable('Crime_Res')

# taking count of resolution against the crime Including resolution in Pending/In-progress/No Resolution
Crime_Resolution_ALL=sqlContext.sql('''select PdDistrict,count(Resolution) as cnt from Crime Group by PdDistrict order by PdDistrict''')
Crime_Resolution_ALL.registerTempTable('Crime_Resolution_ALL')


Crime_Res_ALL=sqlContext.sql('''select * from Crime_Resolution_ALL where cnt<>1 ''').cache()
Crime_Res_ALL.registerTempTable('Crime_Res_ALL')

Crime_corr=sqlContext.sql('''select CR.PdDistrict,CR.cnt as X,CRA.cnt as Y from Crime_Res CR,Crime_Res_ALL CRA where CR.PdDistrict=CRA.PdDistrict ''')
# calculating correlation coefficient among district with respective to their resolution nature
print Crime_corr.stat.corr("X", "Y")

# crime resolution percentage for each district

Crime_percent=sqlContext.sql('''select CR.PdDistrict,CR.cnt/CRA.cnt*100 as cnt from Crime_Res CR,Crime_Res_ALL CRA where CR.PdDistrict=CRA.PdDistrict order by cnt desc''')

#coalescing the final output as it a safe operation to do on our final results-it doesnt affect parallel operation much
Crime_percent.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('Resolution_percent')