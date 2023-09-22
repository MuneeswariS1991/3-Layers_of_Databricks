# 3-Layers_of_Databricks

**Landing Layer : (native Format) -**
This layer is an optional and depends on source systems and data.
Landing is just container in data lake to store raw source data.
This layer represents the area where data land from the data source before processing into delta layers.
Different external systems data ingesting in data lake in native foramt.
Landing is just source systems data in native files like (csv,json,xml,parquet…)
Landing data can be structured , semi-strucutred and un-strucutred files.
Landing data comes from Different sources as a Batch/Streaming Process. add numers to each point


**Bronze layer (Delta Format)**
source data converted and loaded as delta format
everyday data will be appended in delta tables.
bronze tabels are partitioned with updated_date/load_Date to get better performance.
Different external source systems data managed in bronze layer.
The table structures in this layer correspond to the source system table structures “as-is,”.
Bronze tabels will have additional metadata columns that capture the load date/time, process ID, etc.
The focus in this layer is quick Change Data Capture and the ability to provide an historical archive of source (cold storage).
Bronze can be used for reload scenarios in future.
All Historical data will be managed here with audit columns.


**Silver Layer (Delta Format)**
source data converted and loaded as delta format
everyday data will be appended in delta tables.
Uses DeltaLake tables (with SQL table names)
Preserves grain of original data (no aggregation)
Eliminates duplicate records
Production schema enforced
Data quality checks passed
Corrupt data quarantined
Data stored to support production workloads
Optimized for long-term retention and ad-hoc queries
Validate data quality and schema
Enrich and transform data


**Gold layer (Delta Format)**
source data converted and loaded as delta format
everyday data will be appended in delta tables.
Validated and business-level tables
lakehouse is typically organized in consumption-ready “project-specific” databases.
The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins.
The final layer of data transformations and data quality rules are applied here.
Final presentation layer of projects are business data wise models.
We see a lot of Kimball style star schema-based data models or Inmon style Data marts fit in this Gold Layer of the lakehouse.
Validated and business-level tables
lakehouse is typically organized in consumption-ready “project-specific” databases.
The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins.
The final layer of data transformations and data quality rules are applied here.
Final presentation layer of projects are business data wise models.
We see a lot of Kimball style star schema-based data models or Inmon style Data marts fit in this Gold Layer of the lakehouse.
