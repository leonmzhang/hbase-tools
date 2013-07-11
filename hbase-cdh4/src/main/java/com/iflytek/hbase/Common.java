package com.iflytek.hbase;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.log4j.PropertyConfigurator;

public class Common {
  private static void initLogProperties() {
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
  
  public static void globalInit() {
    initLogProperties();
  }
  
  public static CommandLine parseOptions(Options options, String[] args)
      throws Exception {
    Parser paser = new GnuParser();
    return paser.parse(options, args);
  }
}
