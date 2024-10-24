package com.alicloud.tablestore.adaptor.filter;

import com.alicloud.tablestore.adaptor.filter.OFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;


public class OColumnRangeFilter implements OFilter {
    protected byte[] minColumn = null;
    protected boolean minColumnInclusive = true;
    protected byte[] maxColumn = null;
    protected boolean maxColumnInclusive = false;

    public OColumnRangeFilter(byte[] minColumn, boolean minColumnInclusive, byte[] maxColumn, boolean maxColumnInclusive) {
        this.minColumn = minColumn;
        this.minColumnInclusive = minColumnInclusive;
        this.maxColumn = maxColumn;
        this.maxColumnInclusive = maxColumnInclusive;
    }

    public OColumnRangeFilter(ColumnRangeFilter hbaseColumnRangeFilter) {
        this(hbaseColumnRangeFilter.getMinColumn(), hbaseColumnRangeFilter.getMinColumnInclusive(),
                hbaseColumnRangeFilter.getMaxColumn(), hbaseColumnRangeFilter.getMaxColumnInclusive());
    }

    public boolean isMinColumnInclusive() {
        return this.minColumnInclusive;
    }

    public boolean isMaxColumnInclusive() {
        return this.maxColumnInclusive;
    }

    public byte[] getMinColumn() {
        return this.minColumn;
    }

    public boolean getMinColumnInclusive() {
        return this.minColumnInclusive;
    }

    public byte[] getMaxColumn() {
        return this.maxColumn;
    }

    public boolean getMaxColumnInclusive() {
        return this.maxColumnInclusive;
    }
}

