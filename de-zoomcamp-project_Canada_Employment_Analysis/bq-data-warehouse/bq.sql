CREATE OR REPLACE EXTERNAL TABLE `canada_labor_data.canada_labor_data_2019`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-dezoomcamp-project1/canada_labor_data_2019/*'],
  hive_partition_uri_prefix = 'gs://mage-dezoomcamp-project1/canada_labor_data_2019',
  require_hive_partition_filter = false);


CREATE OR REPLACE TABLE `canada_labor_data.canada_labor_data_2019_table`
AS SELECT * FROM `canada_labor_data.canada_labor_data_2019`;

CREATE OR REPLACE TABLE `canada_labor_data.canada_labor_data_2019_part_table`
PARTITION BY RANGE_BUCKET(ref_date, GENERATE_ARRAY(2019, 9999))
AS SELECT * FROM `canada_labor_data.canada_labor_data_2019_table`;

SELECT COUNT(*) as rowcount
FROM `mage-terraform-gcp.canada_labor_data.canada_labor_data_2019_part_table` LIMIT 1000;