DROP TABLE IF EXISTS test_hbase_avro;
CREATE EXTERNAL TABLE test_hbase_avro
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = ":key,cf:rec",
  "cf.rec.serialization.type"="avro",
  "cf.rec.avro.schema.literal"="{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.cloudera.sa.examples.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favourite_color\",\"type\":\"string\"}]}"
)
TBLPROPERTIES ("hbase.table.name" = "users", "hbase.struct.autogenerate"="true");

DROP VIEW IF EXISTS test_hbase_avro_view;
CREATE VIEW test_hbase_avro_view
AS
SELECT
  key as rowkey,
  cf_rec.name as user_name,
  cf_rec.favourite_color as fav_colour
FROM test_hbase_avro;

