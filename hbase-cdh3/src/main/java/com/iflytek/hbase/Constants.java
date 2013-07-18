package com.iflytek.hbase;

public class Constants {
  public static final String LINE_SEPARATOR = System
      .getProperty("line.separator");
  public static final String TAB = "\t";
  public static final String DOUBLE_TAB = "\t\t";
  public static final String TRIPLE_TAB = "\t\t\t";
  
  public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  public static final String SRC_HBASE_ZOOKEEPER_QUORUM = "src.hbase.zookeeper.quorum";
  public static final String DES_HBASE_THRIFT_SERVERS = "des.hbase.thrift.servers";
  public static final String SCAN_MIN_DATE = "scan.min.date";
  public static final String SCAN_MAX_DATE = "scan.max.date";
}
