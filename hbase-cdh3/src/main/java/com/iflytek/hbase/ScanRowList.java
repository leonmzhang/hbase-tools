package com.iflytek.hbase;

import java.io.File;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

public class ScanRowList {
  private static final Log LOG = LogFactory.getLog(ScanRowList.class);
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    String baseDir = System.getProperty("base.dir");
    PropertyConfigurator.configure(baseDir + "/conf/log4j.properties");
    
    String zkQuonum = null;
    if (args.length == 0) {
      System.out.println("input area!");
      System.exit(-1);
    } else if ("gz".equals(args[0])) {
      zkQuonum = Constants.GZ_HBASE_ZOOKEEPER_QUORUM;
    } else if ("hf".equals(args[0])) {
      zkQuonum = Constants.HF_HBASE_ZOOKEEPER_QUORUM;
    } else if ("bj".equals(args[0])) {
      zkQuonum = Constants.BJ_HBASE_ZOOKEEPER_QUORUM;
    } else {
      System.out.print("wrong area!");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    conf.set(Constants.HBASE_ZOOKEEPER_QUORUM,
        Constants.GZ_HBASE_ZOOKEEPER_QUORUM);
    
    File file = new File("scan_list");
    try {
      PrintWriter pw = new PrintWriter(file);
      
      HTable table = new HTable(conf, "personal");
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      Result result = null;
      String rowkey = null;
      String lastScanRow = null;
      
      do {
        try {
          result = scanner.next();
          if (result == null) {
            break;
          }
        } catch (Exception e) {
          LOG.warn("get scanner next failed, last row key: " + lastScanRow, e);
          char lastChar = lastScanRow.charAt(lastScanRow.length() - 1);
          lastChar += 1;
          lastScanRow = lastScanRow.substring(0, lastScanRow.length() - 1)
              + lastChar;
          scan.setStartRow(Bytes.toBytes(lastScanRow));
          scanner.close();
          scanner = table.getScanner(scan);
        }
        rowkey = Bytes.toString(result.getRow());
        lastScanRow = rowkey;
        pw.write(rowkey);
        pw.write(Constants.LINE_SEPARATOR);
        pw.flush();
      } while (result != null);
      
      pw.close();
    } catch (Exception e) {
      LOG.warn("", e);
    }
    
  }
  
}
