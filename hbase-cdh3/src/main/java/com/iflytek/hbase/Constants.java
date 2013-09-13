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
  
  public static final String GZ_HBASE_ZOOKEEPER_QUORUM = "192.168.150.16,192.168.150.17,"
      + "192.168.150.18,192.168.150.19,192.168.150.20";
  public static final String BJ_HBASE_ZOOKEEPER_QUORUM = "192.168.71.47,192.168.71.239,"
      + "192.168.71.241,192.168.71.243,192.168.71.245,192.168.71.248,192.168.71.249";
  public static final String HF_HBASE_ZOOKEEPER_QUORUM = "192.168.52.13,192.168.52.14,"
      + "192.168.52.15,192.168.52.16,192.168.52.17";
  public static final String GZ_HBASE_THRIFT_SERVERS = "192.168.150.22,192.168.150.23,"
      + "192.168.150.24,192.168.150.25,192.168.150.200,192.168.150.201,192.168.150.202";
  public static final String BJ_HBASE_THRIFT_SERVERS = "192.168.151.103,192.168.151.104,"
      + "192.168.151.105,192.168.151.106,192.168.151.107,192.168.151.108,192.168.151.109,"
      + "192.168.151.110";
  public static final String HF_HBASE_THRIFT_SERVERS = "192.168.52.232,192.168.52.233,"
      + "192.168.52.234,192.168.52.248";
  
}
