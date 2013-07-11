package com.iflytek.hbase;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

class CellGet {
  public static final Log LOG = LogFactory.getLog(CellGet.class);
  public static final String ZK_QUORUM = "mirage-pro.hbase0001.gz.voicecloud.cn";
  
  public static final String LINE_SEPARATOR = System.getProperty("line.separator");
  public static final String TAB = "\t";
  
  public static void initLogProperties() {
    Properties properties = new Properties();
    properties.setProperty("log4j.rootLogger", "DEBUG, CONSOLE");
    properties.setProperty("log4j.appender.CONSOLE",
        "org.apache.log4j.ConsoleAppender");
    properties.setProperty("log4j.appender.CONSOLE.layout",
        "org.apache.log4j.PatternLayout");
    properties.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern",
        "%d{yyyy-MM-dd HH:mm:ss} [%5p] %C %M %t:%L:%x - %m%n");
    PropertyConfigurator.configure(properties);
  }
  
  public static void initCmdOptions() {
    Options options = new Options();
    options.addOption("n", "number", true, "The number of scan count!");
    options.addOption("", "", true, "");
    options.addOption("", "", true, "");
  }
  
  public static void usage() {
    StringBuilder sb = new StringBuilder();
    sb.append("Usage: hbase-tools [--config confdir] COMMAND");
    sb.append(LINE_SEPARATOR);
    sb.append("where command is one of:");
    sb.append(LINE_SEPARATOR);
    sb.append(TAB);
    sb.append("scan");
    System.out.println(sb.toString());
  }
  
  public static void main(String[] args) throws Exception {
    initLogProperties();
    if(args.length == 0) {
      usage();
      System.exit(1);
    }
    LOG.info("tool start!");
    
    
    System.out.println("");
    
    LOG.info("tool end");
  }
}
