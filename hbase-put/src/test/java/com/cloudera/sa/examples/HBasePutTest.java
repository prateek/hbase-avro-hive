package com.cloudera.sa.examples;

import com.cloudera.sa.examples.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.cloudera.sa.examples.HBasePut.addUsers;
import static com.cloudera.sa.examples.HBasePut.createTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HBasePutTest {

  private HBaseTestingUtility utility;

  @Before
  public void setUp() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    utility.shutdownMiniCluster();
    utility = null;
  }

  @Test
  public void testCreateTable() throws Exception {
    Configuration config = utility.getConfiguration();
    createTable(config);
    assertTrue(utility.getHBaseAdmin().tableExists("users"));
    HTableDescriptor ht = utility.getHBaseAdmin().getTableDescriptor(Bytes.toBytes("users"));
    HColumnDescriptor[] cols = ht.getColumnFamilies();
    assertEquals(1, cols.length);
    HColumnDescriptor hc = cols[0];
    assertEquals("cf", hc.getNameAsString());
  }

  @Test
  public void testAddUsers() throws Exception {
    Configuration config = utility.getConfiguration();
    createTable(config);
    addUsers(config);
    Table t = utility.getConnection().getTable(TableName.valueOf("users"));
    assertEquals(4, utility.countRows(t));
    User expectedUser = User.newBuilder().setName("John").setFavouriteColor("Red").build();
    Get g1 = new Get("1".getBytes());
    g1.addColumn("cf".getBytes(), "rec".getBytes());
    Result r = t.get(g1);
    byte[] bytes = r.getValue("cf".getBytes(), "rec".getBytes());
    assertEquals(expectedUser, convertBytesToUser(bytes));
  }

  private User convertBytesToUser(byte [] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
    DataFileStream<User> df = new DataFileStream<User>(bais, userDatumReader);
    User user = df.next();
    df.close();
    return user;
  }
}