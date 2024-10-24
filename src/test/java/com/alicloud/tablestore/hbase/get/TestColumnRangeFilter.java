package com.alicloud.tablestore.hbase.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestColumnRangeFilter {

    private static Table table = null;
    private static String familyName = null;
    private static final String rowPrefix = "test_filter_";

    public TestColumnRangeFilter() throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        familyName = config.get("hbase.client.tablestore.family");

        TableName tableName = TableName.valueOf(config.get("hbase.client.tablestore.table"));
        if (!connection.getAdmin().tableExists(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            connection.getAdmin().createTable(descriptor);
            TimeUnit.SECONDS.sleep(1);
        }
        table = connection.getTable(tableName);
    }

    private void clean() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanResult = table.getScanner(scan);

        for (Result row : scanResult) {
            if (row.getRow() == null) continue;
            Delete delete = new Delete(row.getRow());
            table.delete(delete);
        }
    }

    @Test
    public void testGet() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new ColumnRangeFilter(Bytes.toBytes("col_1"), true,
                    Bytes.toBytes("col_3"), false);
            get.setFilter(filter);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(1, cells.size());
            assertEquals("col_1_var",
                    Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(1, cells.size());
            assertEquals("col_2_var",
                    Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(0, cells.size());
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new ColumnRangeFilter(Bytes.toBytes("col_2"), false,
                    Bytes.toBytes("col_4"), true);
            get.setFilter(filter);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(1, cells.size());
            assertEquals("col_3_var",
                    Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(1, cells.size());
            assertEquals("col_4_var",
                    Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

        }
    }
}
