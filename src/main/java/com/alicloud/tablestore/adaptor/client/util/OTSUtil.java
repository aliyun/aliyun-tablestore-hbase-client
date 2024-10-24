package com.alicloud.tablestore.adaptor.client.util;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.tablestore.adaptor.DoNotRetryIOException;
import com.alicloud.tablestore.adaptor.client.OTSErrorCode;
import com.alicloud.tablestore.adaptor.filter.*;
import com.alicloud.tablestore.hbase.ColumnMapping;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class OTSUtil {
    private static final Log LOG = LogFactory.getLog(OTSUtil.class);

    public static PrimaryKey toPrimaryKey(byte[] rowKey, String rowKeyName) {
        PrimaryKeyColumn[] primaryKeyColumns = new PrimaryKeyColumn[1];
        primaryKeyColumns[0] = new PrimaryKeyColumn(rowKeyName, PrimaryKeyValue.fromBinary(rowKey));
        return new PrimaryKey(primaryKeyColumns);
    }

    public static TimeRange toTimeRange(com.alicloud.tablestore.adaptor.struct.OTimeRange timeRange) {
        return new TimeRange(timeRange.getMin(), timeRange.getMax());
    }

    private static SingleColumnValueFilter.CompareOperator toCompareOperator(
            OSingleColumnValueFilter.OCompareOp compareOp) {
        switch (compareOp) {
            case LESS:
                return SingleColumnValueFilter.CompareOperator.LESS_THAN;
            case LESS_OR_EQUAL:
                return SingleColumnValueFilter.CompareOperator.LESS_EQUAL;
            case EQUAL:
                return SingleColumnValueFilter.CompareOperator.EQUAL;
            case GREATER_OR_EQUAL:
                return SingleColumnValueFilter.CompareOperator.GREATER_EQUAL;
            case GREATER:
                return SingleColumnValueFilter.CompareOperator.GREATER_THAN;
            case NOT_EQUAL:
                return SingleColumnValueFilter.CompareOperator.NOT_EQUAL;
            default:
                return null;
        }
    }

    public static ColumnValueFilter toColumnValueFilter(RowQueryCriteria criteria, OFilter filter) {
        Preconditions.checkNotNull(filter);
        if (filter instanceof OSingleColumnValueFilter) {
            OSingleColumnValueFilter oSingleColumnValueFilter = (OSingleColumnValueFilter) filter;
            String columnName = ColumnMapping.getTablestoreColumnName(oSingleColumnValueFilter.getQualifier());
            SingleColumnValueFilter.CompareOperator compareOperator =
                    toCompareOperator(oSingleColumnValueFilter.getOperator());
            ColumnValue columnValue =
                    ColumnValue.fromBinary(oSingleColumnValueFilter.getValue());
            SingleColumnValueFilter singleColumnValueFilter =
                    new SingleColumnValueFilter(columnName, compareOperator, columnValue);
            // passIfMissing = !filterIfMissing
            singleColumnValueFilter.setPassIfMissing(!((OSingleColumnValueFilter) filter).getFilterIfMissing());
            singleColumnValueFilter.setLatestVersionsOnly(((OSingleColumnValueFilter) filter).getLatestVersionOnly());
            return singleColumnValueFilter;
        } else if (filter instanceof OFilterList) {
            CompositeColumnValueFilter.LogicOperator logicOperator = null;
            switch (((OFilterList) filter).getOperator()) {
                case MUST_PASS_ALL:
                    logicOperator = CompositeColumnValueFilter.LogicOperator.AND;
                    break;
                case MUST_PASS_ONE:
                    logicOperator = CompositeColumnValueFilter.LogicOperator.OR;
            }
            CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(logicOperator);
            for (OFilter filterItem : ((OFilterList) filter).getFilters()) {
                if (mayHasValueFilter(filterItem)) {
                    // we don't need to check sub filters here since checking top filter will automatically check sub filters
                    ColumnValueFilter columnValueFilter = toColumnValueFilter(criteria, filterItem);
                    if (columnValueFilter != null) {
                        compositeFilter.addFilter(columnValueFilter);
                    }
                } else {
                    handleNonValueFilterForRowQueryCriteria(criteria, filterItem);
                }
            }
            if (compositeFilter.getSubFilters().size() < 1) {
                return null;
            } else if (compositeFilter.getSubFilters().size() == 1) {
                // if only one filter, return it, since we don't support only one filter in composite filter
                return compositeFilter.getSubFilters().get(0);
            } else {
                return compositeFilter;
            }
        } else {
            throw new UnsupportedOperationException("Unsupported filter type: " + filter.getClass().getName());
        }
    }

    public static void handleValueFilterForRowQueryCriteria(RowQueryCriteria criteria, OFilter filter) {
        checkFilterAndReturnFilterType(filter);
        ColumnValueFilter columnValueFilter = toColumnValueFilter(criteria, filter);
        if (columnValueFilter != null) {
            criteria.setFilter(columnValueFilter);
        }
    }

    public static void handleNonValueFilterForRowQueryCriteria(RowQueryCriteria criteria, OFilter filter) {
        Preconditions.checkNotNull(filter);
        if (filter instanceof OColumnPaginationFilter) {
            OColumnPaginationFilter oFilter = (OColumnPaginationFilter)filter;
            com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter columnPaginationFilter =
                    new com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter(oFilter.getLimit());
            if (oFilter.getColumnOffset() == null) {
                columnPaginationFilter.setOffset(oFilter.getOffset());
            } else {
                criteria.setStartColumn(ColumnMapping.getTablestoreColumnName(oFilter.getColumnOffset()));
            }
            criteria.setFilter(columnPaginationFilter);
        } else if (filter instanceof OColumnRangeFilter) {
            OColumnRangeFilter oFilter = (OColumnRangeFilter)filter;
            if (oFilter.getMinColumn() != null) {
                String colName = ColumnMapping.getTablestoreColumnName(oFilter.getMinColumn());
                if (oFilter.isMinColumnInclusive()) {
                    criteria.setStartColumn(colName);
                } else {
                    criteria.setStartColumn(colName + "\0"); // <= colName is same as < colName+1
                }
            }
            if (oFilter.getMaxColumn() != null) {
                String colName = ColumnMapping.getTablestoreColumnName(oFilter.getMaxColumn());
                if (oFilter.isMaxColumnInclusive()) {
                    criteria.setEndColumn(colName + "\0"); // <= colName is same as < colName+1
                } else {
                    criteria.setEndColumn(colName);
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported filter type: " + filter.getClass().getName());
        }
    }

    public static boolean mayHasValueFilter(OFilter filter) {
        if (filter == null) {
            return false;
        }
        return filter instanceof OSingleColumnValueFilter || filter instanceof OFilterList;
    }

    /**
     * Check whether the filter is valid and return the filter type.
     * If the filter is not valid, throw exception.
     * @param filter input filter to be checked
     * @return 0: single column value filter; 1: column pagination filter; 2: column range filter;
     *         3: filter list with column pagination filter and column range filter;
     */
    public static int checkFilterAndReturnFilterType(OFilter filter) {
        Preconditions.checkNotNull(filter);
        if (filter instanceof OSingleColumnValueFilter) {
            return 0;
        }
        if (filter instanceof OColumnPaginationFilter) {
            return 1;
        }
        if (filter instanceof OColumnRangeFilter) {
            return 2;
        }

        if (filter instanceof OFilterList) {
            int columnPaginationFilterCount = 0;
            int columnRangeFilterCount = 0;
            for (OFilter filterItem : ((OFilterList) filter).getFilters()) {
                int type = checkFilterAndReturnFilterType(filterItem);
                switch (type) {
                    case 0:
                        break;
                    case 1:
                        if (filterItem instanceof OFilterList) {
                            throw new UnsupportedOperationException(
                                    "Unsupported filter type: ColumnPaginationFilter in nesting FilterList");
                        }
                        if (columnPaginationFilterCount > 0) {
                            throw new UnsupportedOperationException(
                                    "Unsupported filter type: multiple ColumnPaginationFilters in FilterList");
                        }
                        columnPaginationFilterCount++;
                        break;
                    case 2:
                        if (filterItem instanceof OFilterList) {
                            throw new UnsupportedOperationException(
                                    "Unsupported filter type: ColumnRangeFilter in nesting FilterList");
                        }
                        if (columnRangeFilterCount > 0) {
                            throw new UnsupportedOperationException(
                                    "Unsupported filter type: multiple ColumnRangeFilters in FilterList");
                        }
                        columnRangeFilterCount++;
                        break;
                    case 3:
                        if (filterItem instanceof OFilterList) {
                            throw new UnsupportedOperationException(
                                    "Unsupported filter type: non-value filter in nesting FilterList");
                        }
                        if (columnPaginationFilterCount > 0) {
                            throw new UnsupportedOperationException(
                                    "Unsupported filter type: multiple ColumnPaginationFilters in FilterList");
                        }
                        if (columnRangeFilterCount > 0) {
                            throw new UnsupportedOperationException(
                                    "Unsupported filter type: multiple ColumnRangeFilters in FilterList");
                        }
                        columnPaginationFilterCount++;
                        columnRangeFilterCount++;
                        break;
                    default:
                        throw new IllegalStateException("Unknown filter type");
                }
            }
            // if non-value filter is in the filter list, we can only handle it with MUST_PASS_ALL
            if (columnPaginationFilterCount + columnRangeFilterCount > 0) {
                if (((OFilterList) filter).getOperator() != OFilterList.Operator.MUST_PASS_ALL) {
                    throw new UnsupportedOperationException(
                            "Unsupported filter type: non-value filter in filter list without MUST_PASS_ALL");
                }
                if (columnRangeFilterCount == 0) {
                    return 1;
                }
                if (columnPaginationFilterCount == 0) {
                    return 2;
                }
                return 3;
            }
            return 0;
        }
        throw new UnsupportedOperationException("Unsupported filter type: " + filter.getClass().getName());
    }

    public static void handleFilterForRowQueryCriteria(RowQueryCriteria criteria, OFilter filter) {
        if (filter == null) {
            return;
        }
        if (mayHasValueFilter(filter)) {
            handleValueFilterForRowQueryCriteria(criteria, filter);
        } else {
            handleNonValueFilterForRowQueryCriteria(criteria, filter);
        }
    }

    public static com.alicloud.tablestore.adaptor.struct.OResult parseOTSRowToResult(Row row) {
        if (row == null) {
            return new com.alicloud.tablestore.adaptor.struct.OResult(null, new com.alicloud.tablestore.adaptor.struct.OColumnValue[0]);
        }
        byte[] rowKey = row.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary();
        int columnNum = row.getColumns().length;
        com.alicloud.tablestore.adaptor.struct.OColumnValue[] kvs = new com.alicloud.tablestore.adaptor.struct.OColumnValue[columnNum];

        for (int i = 0; i < columnNum; i++) {
            kvs[i] =
                    new com.alicloud.tablestore.adaptor.struct.OColumnValue(rowKey,
                            Bytes.toBytes(row.getColumns()[i].getName()), row.getColumns()[i].getTimestamp(),
                            com.alicloud.tablestore.adaptor.struct.OColumnValue.Type.PUT, row.getColumns()[i].getValue().asBinary());
        }
        return new com.alicloud.tablestore.adaptor.struct.OResult(rowKey, kvs);
    }

    public static boolean shouldRetry(Throwable ex) {
        if (ex instanceof TableStoreException) {
            String errorCode = ((TableStoreException) ex).getErrorCode();
            if (errorCode.equals(OTSErrorCode.INVALID_PARAMETER)
                    || errorCode.equals(OTSErrorCode.AUTHORIZATION_FAILURE)
                    || errorCode.equals(OTSErrorCode.INVALID_PK)
                    || errorCode.equals(OTSErrorCode.OUT_OF_COLUMN_COUNT_LIMIT)
                    || errorCode.equals(OTSErrorCode.OUT_OF_ROW_SIZE_LIMIT)
                    || errorCode.equals(OTSErrorCode.CONDITION_CHECK_FAIL)
                    || errorCode.equals(OTSErrorCode.REQUEST_TOO_LARGE)) {
                return false;
            }
        }
        return true;
    }
}
