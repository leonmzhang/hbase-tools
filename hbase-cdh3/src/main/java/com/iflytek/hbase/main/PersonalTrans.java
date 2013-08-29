package com.iflytek.hbase.main;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

public class PersonalTrans {
  private static final Log LOG = LogFactory.getLog(PersonalTrans.class);
  private static int CHECK_INTERVAL = 10240;
  private static String LINE_END = System.getProperty("line.separator");
  
  private static String baseDir = null;
  private static String outputDir = null;
  private static String outputPrefix = "check_";
  
  private HTablePool tablePool = null;
  
  
  
  public int runTool(Configuration conf, String[] args) {
    tablePool = new HTablePool(conf, 128);
    HTableInterface table = tablePool.getTable("personal");
    
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("cf"));
    ResultScanner scanner = null;
    Result result = null;
    byte[] row = null;
    String rowStr = null;
    PrintWriter pw = null;
        
    try {
      scanner = table.getScanner(scan);
      pw = new PrintWriter(new File(outputDir + "/" + outputPrefix + "01"));
      int tempCount = CHECK_INTERVAL;
      while((result = scanner.next()) != null && tempCount-- > 0) {
        row = result.getRow();
        rowStr = Bytes.toString(row);
        pw.write(rowStr + LINE_END);
        pw.flush();
      }
      
    } catch (IOException e) {
      LOG.info("", e);
    } finally {
      if(scanner != null){
        scanner.close();
      }
    }
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("");
      System.exit(-1);
    }
    baseDir = System.getProperty("base.dir");
    outputDir = baseDir + "/check_data/";
    File outputDirFile = new File(outputDir);
    if(outputDirFile.exists()) {
      outputDirFile.mkdirs();
    }
    PropertyConfigurator.configure(baseDir + "/../conf/log4j.properties");
    LOG.info("start");
    
    Configuration conf = new Configuration();
    conf.addResource("hbase-tools.xml");
    
    PersonalTrans pt = new PersonalTrans();
    pt.runTool(conf, args);
    
    LOG.info("stop");
  }
  
}
