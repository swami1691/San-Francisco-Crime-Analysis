import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
import datetime

inputPath = '/user/ssambasi/SFPD_parquet'
conf = SparkConf().setAppName('Crime Association and Dependency')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crimeDF =sqlContext.read.parquet(inputPath)
crimeRDD = crimeDF.rdd.cache()

outputPath1 = '/user/ssambasi/sfo/AssociatedCrimes/'
outputPath2 = '/user/ssambasi/sfo/CrimeDependency/'


#Incidents with categories
incidentCategoryRDD = crimeRDD.map(lambda row: (row['IncidntNum'],row['Category']))\
                      .reduceByKey(lambda c1,c2: c1+';'+c2).map(lambda (IncidntNum,Categories):\
                                (IncidntNum,Categories.split(';'))).cache()

#Categories List
categoriesCountRDD = crimeRDD.map(lambda row: (row['Category'],1)).reduceByKey(lambda a,b: a+b).cache()
categoriesCountList = categoriesCountRDD.collect()
categoriesList = categoriesCountRDD.map(lambda (Category,a):Category).collect()

combinationalCategoriesList = []
categoryDependencyList = []
duplicatecategoriesList = categoriesList[:]
for c1 in categoriesList:
    duplicatecategoriesList.remove(c1)
    for c2 in duplicatecategoriesList:
        incidentsCount = incidentCategoryRDD.filter(lambda (IncidntNum,Categories): \
                                                    (c1 in Categories) and (c2 in Categories)).count()
        if (incidentsCount>0):
            c1Count = filter(lambda (category,count): category==c1, categoriesCountList)[0][1]
            c2Count = filter(lambda (category,count): category==c2, categoriesCountList)[0][1]
            
            combinationalCategoriesList.append(Row(Category1=c1,Category2=c2,IncidentsCount=incidentsCount))
            
            categoryDependencyList.append(Row(Category=c1,DependsOn=c2,Dependency=float(incidentsCount)/c1Count))
            categoryDependencyList.append(Row(Category=c2,DependsOn=c1,Dependency=float(incidentsCount)/c2Count))

combinationalCategoriesDF = sc.parallelize(combinationalCategoriesList).toDF().sort("IncidentsCount", ascending=False).cache()
categoryDependencyDF = sc.parallelize(categoryDependencyList).toDF().sort("Dependency", ascending=False).cache()

categoryDependencyDF = sc.parallelize(categoryDependencyList).toDF().sort("Dependency", ascending=False).cache()

#Save Associated Crimes
combinationalCategoriesDF.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save(outputPath1)

#Save Top 5 Dependent Crimes
categoryDependencyDF = sc.parallelize(categoryDependencyDF.take(5)).toDF()
categoryDependencyDF.coalesce(1).write.mode('append').json(outputPath2)
