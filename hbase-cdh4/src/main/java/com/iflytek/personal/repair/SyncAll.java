package com.iflytek.personal.repair;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

public class SyncAll {
  private static final Log LOG = LogFactory.getLog(SyncAll.class);
  
  private static final Configuration bjConf = new Configuration();
  private static final Configuration hfConf = new Configuration();
  private static final Configuration gzConf = new Configuration();
  
  private static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  
  private static final String BJ_ZK_QUORUM = "";
  private static final String HF_ZK_QUORUM = "";
  private static final String GZ_ZK_QUORUM = "";
  
  private static final String TABLE_NAME = "personal";
  private static final String FAMILY = "p";
  
  private static final AtomicInteger count = new AtomicInteger(0);
  
  static {
    bjConf.set(HBASE_ZOOKEEPER_QUORUM, BJ_ZK_QUORUM);
    hfConf.set(HBASE_ZOOKEEPER_QUORUM, HF_ZK_QUORUM);
    gzConf.set(HBASE_ZOOKEEPER_QUORUM, GZ_ZK_QUORUM);
  }
  
  private HTable bjTable = null;
  private HTable hfTable = null;
  private HTable gzTable = null;
  
  public int runTool() throws IOException {
    setup();
    
    Result result = null;
    Scan scan = new Scan();
    
    ResultScanner scanner = bjTable.getScanner(scan);
    String lastScanRow = null;
    Map<?,?> familyMap = null;
    byte[] row = null;
    byte[] family = Bytes.toBytes(FAMILY);
    byte[] qualifier = null;
    byte[] value = null;
    long timestamp = 0;
    
    do {
      try {
        result = scanner.next();
        if (result == null) {
          break;
        }
      } catch (Exception e) {
        LOG.warn("", e);
        char lastChar = lastScanRow.charAt(lastScanRow.length() - 1);
        lastChar += 1;
        lastScanRow = lastScanRow.substring(0, lastScanRow.length() - 1)
            + lastChar;
        scan.setStartRow(Bytes.toBytes(lastScanRow));
        scanner.close();
        scanner = bjTable.getScanner(scan);
        continue;
      }
      familyMap = result.getFamilyMap(Bytes.toBytes(FAMILY));
      row = result.getRow();
      for(Map.Entry<?,?> entry : familyMap.entrySet()) {
        qualifier = (byte[])entry.getKey();
        value = (byte[])entry.getValue();
        timestamp = result.getColumnLatest(family, qualifier).getTimestamp();
        checkData(hfTable, row, family, qualifier, timestamp, value);
      }
      
    } while (result != null);
    
    cleanup();
    return 0;
  }
  
  private void setup() throws IOException {
    bjTable = new HTable(bjConf, TABLE_NAME);
    hfTable = new HTable(hfConf, TABLE_NAME);
    gzTable = new HTable(gzConf, TABLE_NAME);
  }
  
  private void cleanup() throws IOException {
    bjTable.close();
    hfTable.close();
    gzTable.close();
  }
  
  private void checkData(HTable table, byte[] row, byte[] family,
      byte[] qualifier, long timestamp, byte[] value) {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    
    Result result = null;
    try {
      result = table.get(get);
      if (result == null) {
        Put put = new Put(row);
        put.add(family, qualifier, timestamp, value);
        // table.put(put);
      }
    } catch (IOException e) {
      LOG.warn("", e);
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    PropertyConfigurator.configure("");
    
    SyncAll sa = new SyncAll();
    try {
      sa.runTool();
    } catch (Exception e) {
      LOG.warn("", e);
    }
  }
}
