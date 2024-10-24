package com.alicloud.tablestore.adaptor.struct;


import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.client.util.OTSUtil;
import com.alicloud.tablestore.hbase.ColumnMapping;
import org.apache.hadoop.hbase.client.WrongRowIOException;

import java.io.IOException;
import java.util.Arrays;

public class OUpdate extends OMutation implements Comparable<ORow> {
    public OUpdate(byte[] row) {
        Preconditions.checkNotNull(row);

        this.row = Arrays.copyOf(row, row.length);
    }

    public OUpdate(OUpdate updateToCopy) {
        this(updateToCopy.getRow());
        this.keyValues.addAll(updateToCopy.keyValues);
    }

    public void add(OPut p) throws IOException {
        internalAdd(p);
    }

    public void add(ODelete d) throws IOException {
        internalAdd(d);
    }

    private void internalAdd(OMutation m) throws IOException {
        int res = Bytes.compareTo(this.row, m.getRow());
        if (res != 0) {
            throw new WrongRowIOException("The row in the recently added Put/Delete <" +
                    Bytes.toStringBinary(m.getRow()) + "> doesn't match the original one <" +
                    Bytes.toStringBinary(this.row) + ">");
        }
        this.keyValues.addAll(m.getKeyValues());
    }

    public RowUpdateChange toOTSParameter(String tableName) throws IOException {
        PrimaryKey primaryKey = OTSUtil.toPrimaryKey(getRow(), OTSConstants.PRIMARY_KEY_NAME);

        RowUpdateChange ruc = new RowUpdateChange(tableName, primaryKey);
        for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
            switch (kv.getType()) {
                case PUT:
                    if (kv.getTimestamp() == OTSConstants.LATEST_TIMESTAMP) {
                        ruc.put(ColumnMapping.getTablestoreColumnName(
                                kv.getQualifier()), ColumnValue.fromBinary(kv.getValue()));
                    } else {
                        ruc.put(ColumnMapping.getTablestoreColumnName(
                                kv.getQualifier()), ColumnValue.fromBinary(kv.getValue()), kv.getTimestamp());
                    }
                    break;
                case DELETE:
                    ruc.deleteColumn(ColumnMapping.getTablestoreColumnName(kv.getQualifier()), kv.getTimestamp());
                    break;
                case DELETE_ALL:
                    ruc.deleteColumns(ColumnMapping.getTablestoreColumnName(kv.getQualifier()));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + kv.getType().name());
            }
        }
        ruc.setCondition(getCondition());
        return ruc;
    }
}
