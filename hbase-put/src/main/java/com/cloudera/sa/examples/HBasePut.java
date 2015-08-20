package com.cloudera.sa.examples;

import com.cloudera.sa.examples.avro.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class HBasePut
{
  public static void createTable(Configuration config) throws IOException {
    // create an admin object using the config
    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists("users")) {
      System.out.println("'users' table does not exist, creating it.");
      // create the table...
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("users"));
      tableDescriptor.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(tableDescriptor);
      System.out.println("'users' table created.");
    }
  }

  static byte[] convertAvroToBytes(User user) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<User> datumWriter = new SpecificDatumWriter<User>(User.class);
    DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(datumWriter);
    dataFileWriter.create(user.getSchema(), out);
    dataFileWriter.append(user);
    dataFileWriter.close();
    return out.toByteArray();
  }


  public static void addUsers(Configuration config) throws IOException {
    String[][] users = {
        { "1", "John", "Red"},
        { "2", "Jane", "Grey" },
        { "3", "Mike", "Blue" },
        { "4", "Roger", "Black" }};

    HTable table = new HTable(config, "users");

    for (int i = 0; i< users.length; i++) {
      Put put = new Put(Bytes.toBytes(users[i][0]));
      User user = User.newBuilder()
          .setName(users[i][1])
          .setFavouriteColor(users[i][2])
          .build();
      put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("rec"), convertAvroToBytes(user));
      table.put(put);
    }
    // flush commits and close the table
    table.flushCommits();
    table.close();
  }

  public static void main( String[] args ) throws IOException {
    Configuration config = HBaseConfiguration.create();

    createTable(config);
    addUsers(config);
  }
}
