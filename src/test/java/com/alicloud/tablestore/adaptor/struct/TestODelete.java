/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.tablestore.adaptor.client.TablestoreClientConf;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.hbase.TablestoreConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.tablestore.adaptor.client.OTSConstants;

import java.io.IOException;

public class TestODelete {
  public static final byte[] ROW_01 = Bytes.toBytes("row-01");
  public static final byte[] QUALIFIER_01 = Bytes.toBytes("qualifier-01");
  public static final long TS = 1234567L;
  private final AsyncClientInterface ots;
  private String tableName;

  public TestODelete() throws IOException {
    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    TablestoreConnection otsConnection = (TablestoreConnection)connection;
    TablestoreClientConf conf = otsConnection.getTablestoreConf();
    ClientConfiguration otsConf = new ClientConfiguration();
    otsConf.setMaxConnections(conf.getOTSMaxConnections());
    otsConf.setSocketTimeoutInMillisecond(conf.getOTSSocketTimeout());
    otsConf.setConnectionTimeoutInMillisecond(conf.getOTSConnectionTimeout());
    otsConf.setRetryStrategy(new DefaultRetryStrategy());
    ots = new AsyncClient(conf.getOTSEndpoint(), conf.getTablestoreAccessKeyId(), conf.getTablestoreAccessKeySecret(),
            conf.getOTSInstanceName(), otsConf);
    TableName t = TableName.valueOf(config.get("hbase.client.tablestore.table"));
    tableName = t.getNameAsString();
  }

  @Test
  public void testToOTSParameter() throws Exception {
    // delete row
    ODelete delete = new ODelete(ROW_01);
    RowDeleteChange rowDeleteChange = (RowDeleteChange) delete.toOTSParameter(tableName);
    assertEquals(tableName, rowDeleteChange.getTableName());
    assertEquals(1, rowDeleteChange.getPrimaryKey().size());
    assertEquals(OTSConstants.PRIMARY_KEY_NAME, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getName());
    assertTrue(Bytes.equals(ROW_01, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary()));

    // delete columns
    delete = new ODelete(ROW_01);
    delete.deleteColumns(QUALIFIER_01);
    RowUpdateChange rowUpdateChange = (RowUpdateChange) delete.toOTSParameter(tableName);
    assertEquals(tableName, rowDeleteChange.getTableName());
    assertEquals(1, rowDeleteChange.getPrimaryKey().size());
    assertEquals(OTSConstants.PRIMARY_KEY_NAME, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getName());
    assertTrue(Bytes.equals(ROW_01, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary()));
    assertEquals(1, rowUpdateChange.getColumnsToUpdate().size());
    assertEquals(Bytes.toString(QUALIFIER_01), rowUpdateChange.getColumnsToUpdate().get(0).getFirst().getName());
    assertEquals(RowUpdateChange.Type.DELETE_ALL, rowUpdateChange.getColumnsToUpdate().get(0).getSecond());

    // delete column
    delete = new ODelete(ROW_01);
    delete.deleteColumn(QUALIFIER_01, TS);
    rowUpdateChange = (RowUpdateChange) delete.toOTSParameter(tableName);
    assertEquals(tableName, rowDeleteChange.getTableName());
    assertEquals(1, rowDeleteChange.getPrimaryKey().size());
    assertEquals(OTSConstants.PRIMARY_KEY_NAME, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getName());
    assertTrue(Bytes.equals(ROW_01, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary()));
    assertEquals(1, rowUpdateChange.getColumnsToUpdate().size());
    assertEquals(Bytes.toString(QUALIFIER_01), rowUpdateChange.getColumnsToUpdate().get(0).getFirst().getName());
    assertEquals(TS, rowUpdateChange.getColumnsToUpdate().get(0).getFirst().getTimestamp());
    assertEquals(RowUpdateChange.Type.DELETE, rowUpdateChange.getColumnsToUpdate().get(0).getSecond());

  }

}
