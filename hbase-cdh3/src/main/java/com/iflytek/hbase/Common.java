package com.iflytek.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.log4j.PropertyConfigurator;

public class Common {
  private static String logLevel = "INFO";
  private static String logger = "CONSOLE";
  
  private static void initLogProperties() {
    Properties properties = new Properties();
    properties.setProperty("log4j.rootLogger", logLevel + ", " + logger);
    properties.setProperty("log4j.appender.CONSOLE",
        "org.apache.log4j.ConsoleAppender");
    properties.setProperty("log4j.appender.CONSOLE.layout",
        "org.apache.log4j.PatternLayout");
    properties.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern",
        "%d{yyyy-MM-dd HH:mm:ss} [%5p] %C %M %t:%L:%x - %m%n");
    PropertyConfigurator.configure(properties);
  }
  
  public static void globalInit() {
    initLogProperties();
  }
  
  public static CommandLine parseOptions(Options options, String[] args)
      throws Exception {
    Parser paser = new GnuParser();
    return paser.parse(options, args);
  }
  
  public static String  completionString(String str, int len) {
    return completionString(str, ' ', len, true);
  }
  
  public static String  completionString(String str, char chr, int len, boolean left) {
    if(str ==  null || str.length() >= len) {
      return str;
    }
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < len - str.length(); i++) {
      sb.append(chr);
    }
    
    if(left) {
      sb.append(str);
      return sb.toString();
    } else {
      return str + sb.toString();
    }
  }
  
  public static String unixTimestampToDateStr(long timestamp) {
    return null;
  }
  
  public static long dateStrToUnixTimestamp(String dateStr) {
    if(dateStr == null) {
      return 0;
    }
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      Date date = sdf.parse(dateStr);
      return date.getTime();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return 0;
  }
}
