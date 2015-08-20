HBase-Hive-Avro
===============

This project demonstrates how to use HBase columns containing Avro records via Hive. Hive added new functionality for this use-case under https://issues.apache.org/jira/browse/HIVE-6147, but I couldn't find an example of how to use it, which brings us here.

### First, create and load a HBase table
The folder `hbase-put` contains a Maven project which creates an HBase table named 'user', with a single column family 'cf'. It further loads 4 Avro records into this table/column-family under the qualifier 'rec'. Follow directions below to build and execute:

```sh
$ cd hbase-put
$ mvn package
$ export CP=/etc/hbase/conf:$(hbase classpath):./target/hbase-1.0-SNAPSHOT.jar
$ java -cp $CP com.cloudera.sa.examples.HBasePut
```

### Next, create Hive table and view for the HBase table
The provided file, `setup-hive.hql` creates a Hive table named `test_hbase_avro` using the functionality from the JIRA pointed to above.

To begin with, run the `setup-hive.hql` file:

```sh
$ hive -f setup-hive.hql
```

Now to explore the created tables:

```sh
$ hive -e "desc test_hbase_avro; select * from test_hbase_avro;"

OK
key                     string                  from deserializer
cf_rec                  struct<name:string,favourite_color:string>      from deserializer
Time taken: 2.252 seconds, Fetched: 2 row(s)

OK
1       {"name":"John","favourite_color":"Red"}
2       {"name":"Jane","favourite_color":"Grey"}
3       {"name":"Mike","favourite_color":"Blue"}
4       {"name":"Roger","favourite_color":"Black"}
Time taken: 0.635 seconds, Fetched: 4 row(s)
```

It also creates an additional view, `test_hbase_avro_view` which exposes the same Hive table with columns containing only primitive types, as follows:

```sh
$ hive -e "desc test_hbase_avro_view; select * from test_hbase_avro_view;"

OK
rowkey                  string
user_name               string
fav_colour              string
Time taken: 2.23 seconds, Fetched: 3 row(s)

OK
1       John    Red
2       Jane    Grey
3       Mike    Blue
4       Roger   Black
Time taken: 1.377 seconds, Fetched: 4 row(s)
```

# TODO
- insert followup note about UDTF extensions
