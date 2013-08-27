package com.iflytek.hbase.main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.iflytek.hbase.Common;

public class CheckAppData {
  private static Log LOG = LogFactory.getLog(CheckAppData.class);
  
  private static String LINE_SEPARATOR = System.getProperty("line.separator");
  
  private HTablePool tablePool;
  
  public class AppDataFilter implements Filter {
    
    @Override
    public void readFields(DataInput arg0) throws IOException {
      // TODO Auto-generated method stub
      
    }
    
    @Override
    public void write(DataOutput arg0) throws IOException {
      // TODO Auto-generated method stub
      
    }
    
    @Override
    public void reset() {
      // TODO Auto-generated method stub
      
    }
    
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
      String rowKey = Bytes.toString(buffer);
      if (rowKey.startsWith("app")) {
        return true;
      } else {
        return false;
      }
    }
    
    @Override
    public boolean filterAllRemaining() {
      // TODO Auto-generated method stub
      return false;
    }
    
    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public void filterRow(List<KeyValue> kvs) {
      // TODO Auto-generated method stub
      
    }
    
    @Override
    public boolean hasFilterRow() {
      // TODO Auto-generated method stub
      return false;
    }
    
    @Override
    public boolean filterRow() {
      // TODO Auto-generated method stub
      return false;
    }
    
    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  public int runTool(Configuration conf, String[] args) {
    
    tablePool = new HTablePool(conf, 128);
    HTableInterface table = tablePool.getTable("personal");
    
    Scan scan = new Scan();
    AppDataFilter filter = new AppDataFilter();
    // scan.setFilter(filter);
    scan.addFamily(Bytes.toBytes("cf"));
    scan.setStartRow(Bytes.toBytes("aa1050"));
    
    ResultScanner scanner = null;
    try {
      File output = new File("gz_list");
      PrintWriter pw = new PrintWriter(output);
      
      scanner = table.getScanner(scan);
      
      Result result = null;
      String rowKey = null;
      String scanLastRow = null;
      
      do {
        try {
          result = scanner.next();
          if (result == null) {
            break;
          }
        } catch (Exception e) {
          LOG.warn(
              "scanner timeout, get scanner from last row: " + scanLastRow, e);
          scan.setStartRow(Bytes.toBytes(scanLastRow));
          scanner.close();
          scanner = table.getScanner(scan);
          continue;
        }
        
        rowKey = Bytes.toString(result.getRow());
        scanLastRow = rowKey;
        LOG.info(rowKey);
        pw.write(rowKey + LINE_SEPARATOR);
        pw.flush();
        
      } while (result != null);
    } catch (IOException e) {
      LOG.warn("", e);
      return 0;
    }
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    Common.globalInit();
    
    Configuration conf = new Configuration();
    conf.addResource("hbase-tools.xml");
    CheckAppData cad = new CheckAppData();
    try {
      cad.runTool(conf, args);
    } catch (Exception e) {
      LOG.warn("", e);
    }
  }
}
