This is to store all files pertaining to orchestration via Mage.

Essentially this is how the pipeline orchestration looks like.
![tree](https://github.com/yuxiangl6/zoomcamp24/assets/143888207/101df3c9-0261-4f27-ba64-e9966caa6fae)



It's scheduled to run on the 1st calendar day of every year.
![pipeline-scheduled-run](https://github.com/yuxiangl6/zoomcamp24/assets/143888207/bd15b414-b089-405b-9af1-cb069005252a)




(1) DATA LOAD PROCESS
I've selected this dataset "https://github.com/thefabscientist/Waterloo-DS3-Group-1-Project/blob/main/CanadaLabourData.csv.gz?raw=true", 
which contains the Canada Labor Data from 1970s to 2020.

However, to save space in Codespace (max 32 GB), I filtered data to year 2019 using pandas in this step first because dataset is huge (400MB).

I dropped ['status', 'symbol', 'terminated', 'terminated'] columns as they basically contains the same values for all data rows,
so it's not significant

Refer to data-load.py for script.

Data load output:
![data-load-output](https://github.com/yuxiangl6/zoomcamp24/assets/143888207/a2af9619-38ad-41fa-a7d3-3767078d748c)






(2) TRANSFORMATION PROCESS
I re-defined the schema from pandas to spark, created spark dataframes and dropped `decimal` column as it doesn't faciliate much in data analysis.

To ease data export process later, I converted the spark dataframes back to pandas, as it's easier to do so via pandas.

Refer to data-spark-transform.py for script.

Data transform output:
![data-spark-transform-output](https://github.com/yuxiangl6/zoomcamp24/assets/143888207/66907951-661f-4040-8c66-d3b82c7ef390)






(3) DATA EXPORT TO GCP STORAGE
I published the data to GCP, using field `ref_date` as partitions 

Refer to data-export-gcs-partitioned.py for script.

Data export to GCP output:
![data-export-gcs-partitoned](https://github.com/yuxiangl6/zoomcamp24/assets/143888207/a191c7fc-15bf-46f0-8fbc-20b10c5ba91a)



