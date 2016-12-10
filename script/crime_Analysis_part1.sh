spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster load_crime.py  /user/chandras/SFPD.csv SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Crime_Vehicle_Theft.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster District_Time_Crime.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Month_Crime.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Resolution_Percentage.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Hive_Month.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Hive_Time.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Hive_District.py /user/chandras/SFPD_parquet