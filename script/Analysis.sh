
spark-submit  --packages com.databricks:spark-csv_2.11:1.3.0 --master yarn-cluster DistrictResolution.py

spark-submit  --packages com.databricks:spark-csv_2.11:1.3.0 --master yarn-cluster IncidentWeights.py

spark-submit  --packages com.databricks:spark-csv_2.11:1.3.0 --master yarn-cluster ResolutionOnMap.py

