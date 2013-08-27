package com.iflytek.hbase.main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.iflytek.hbase.Common;

public class CheckAppData {
  private static Log LOG = LogFactory.getLog(CheckAppData.class);
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
  
  public int runTool(Configuration conf, String[] args) throws Exception {
    tablePool = new HTablePool(conf, 128);
    HTableInterface table = tablePool.getTable("personal");
    
    Scan scan = new Scan();
    AppDataFilter filter = new AppDataFilter();
    scan.setFilter(filter);
    scan.addFamily(Bytes.toBytes("cf"));
    ResultScanner scanner = table.getScanner(scan);
    Result result = null;
    String rowKey = null;
    
    do {
      result = scanner.next();
      rowKey = Bytes.toString(result.getRow());
      LOG.info(rowKey);
      
    } while (result != null);
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Common.globalInit();
    
    Configuration conf = new Configuration();
    conf.addResource("hbase-tools.xml");
    CheckAppData cad = new CheckAppData();
    cad.runTool(conf, args);
  }
}
