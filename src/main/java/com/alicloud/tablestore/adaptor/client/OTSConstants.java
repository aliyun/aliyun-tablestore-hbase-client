package com.alicloud.tablestore.adaptor.client;

import org.apache.hadoop.hbase.HConstants;

public class OTSConstants {
  /**
   * An empty instance.
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * Used by scanners, etc when they want to start at the beginning of a region
   */
  public static final byte[] EMPTY_START_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Last row in a table.
   */
  public static final byte[] EMPTY_END_ROW = EMPTY_START_ROW;

  public static final String PRIMARY_KEY_NAME = "__rowkey__";

  public static final String UTF8_ENCODING = "utf-8";

  public static final String USE_UTF8_ENCODING = "hbase.client.tablestore.use_utf8";

  /**
   * In hbase server, LATEST_TIMESTAMP will be converted to EnvironmentEdgeManager.currentTime()
   */
  public static long LATEST_TIMESTAMP = HConstants.LATEST_TIMESTAMP;

  public static final String GLOBAL_FAMILY_CONF_KEY = "hbase.client.tablestore.family";
  public static final String DEFAULT_FAMILY_NAME = "s";

}
