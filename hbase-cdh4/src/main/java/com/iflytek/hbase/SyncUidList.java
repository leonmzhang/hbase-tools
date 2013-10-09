package com.iflytek.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

public class SyncUidList {
  private static final Log LOG = LogFactory.getLog(SyncUidList.class);
  
  private static final String BJ_ZK_QUORUM = "192.168.151.101,192,168.151.102,192.168.151.103";
  private static final String GZ_ZK_QUORUM = "192.168.150.21";
  private static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  
  private static final HashMap<String,String> APPID_MAP = new HashMap<String,String>();
  
  private static final byte[] FAIMILY = Bytes.toBytes("p");
  
  static {
    APPID_MAP.put("4edca818", "");
    APPID_MAP.put("4f310ba1", "");
    APPID_MAP.put("4ed5c3a3", "");
  }
  
  private static String baseDir = "";
  
  private HTable srcTable;
  private HTable desTable;
  
  public int runTool(Configuration srcConf, Configuration desConf, String[] args)
      throws Exception {
    if (args.length != 1) {
      LOG.warn("wrong args");
      return -1;
    }
    
    File listFile = new File(args[0]);
    if (!listFile.exists()) {
      LOG.warn("file nonexist: " + listFile.getAbsolutePath());
      return -1;
    }
    
    srcTable = new HTable(srcConf, "personal");
    desTable = new HTable(desConf, "personal");
    Get srcGet = null;
    Get desGet = null;
    Put desPut = null;
    
    FileReader fr = null;
    BufferedReader br = null;
    String line = null;
    String rowKey = null;
    Result srcResult = null;
    Result desResult = null;
    long srcTimestamp = 0;
    long desTimestamp = 0;
    byte[] srcQualify = null;
    byte[] value = null;
    Map<?,?> srcResultMap = null;
    
    try {
      fr = new FileReader(listFile);
      br = new BufferedReader(fr);
      
      while ((line = br.readLine()) != null) {
        LOG.info("get line: " + line);
        rowKey = getRowKey(line);
        if (rowKey == null) {
          LOG.warn("row key is null");
          continue;
        }
        LOG.info("row key: " + rowKey);
        
        srcGet = new Get(Bytes.toBytes(rowKey));
        
        srcResult = srcTable.get(srcGet);
        if (srcResult.isEmpty()) {
          LOG.info("get row key: " + rowKey
              + " from src table failed, result is empty");
          continue;
        }
        srcResultMap = srcResult.getFamilyMap(Bytes.toBytes("p"));
        for (Map.Entry<?,?> entry : srcResultMap.entrySet()) {
          srcQualify = (byte[]) entry.getKey();
          srcTimestamp = srcResult.getColumnLatest(FAIMILY, srcQualify)
              .getTimestamp();
          LOG.info("row key: " + rowKey + ", get qualify: "
              + Bytes.toString(srcQualify) + ", modify time: "
              + Common.unixTimestampToDateStr(srcTimestamp));
          
          desGet = new Get(Bytes.toBytes(rowKey));
          desGet.addColumn(Bytes.toBytes("p"), srcQualify);
          desResult = desTable.get(desGet);
          value = (byte[]) entry.getValue();
          if (desResult.isEmpty()) {
            LOG.info("des table result is empty, put this cell");
            desPut = new Put(Bytes.toBytes(rowKey));
            desPut.add(FAIMILY, srcQualify, srcTimestamp, value);
            desTable.put(desPut);
          } else {
            desTimestamp = desResult.getColumnLatest(FAIMILY, srcQualify)
                .getTimestamp();
            if (desTimestamp < srcTimestamp
                && (srcTimestamp - desTimestamp > 300000000)) {
              LOG.info("modify time of des table: "
                  + Common.unixTimestampToDateStr(desTimestamp)
                  + " is early than src table more than 5 min, put this cell");
              desPut = new Put(Bytes.toBytes(rowKey));
              desPut.add(FAIMILY, srcQualify, srcTimestamp, value);
              desTable.put(desPut);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("", e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (Exception e) {}
      }
      if (fr != null) {
        try {
          fr.close();
        } catch (Exception e) {}
      }
    }
    
    srcTable.close();
    desTable.close();
    return 0;
  }
  
  private String getRowKey(String line) {
    String rowKey = null;
    String[] strArray = line.split("\t");
    if (strArray.length != 4) {
      return null;
    }
    String uid = strArray[0];
    String appid = strArray[1];
    if (APPID_MAP.get(appid) != null) {
      rowKey = uid + "@" + appid;
    } else {
      rowKey = uid;
    }
    
    return rowKey;
  }
  
  public static void main(String[] args) {
    baseDir = System.getProperty("base.dir");
    LOG.debug("get base dir: " + baseDir);
    if (baseDir == null) {
      LOG.warn("base dir is null");
      return;
    }
    
    PropertyConfigurator.configure(baseDir + "/conf/log4j.properties");
    
    Configuration srcConf = new Configuration();
    srcConf.set(HBASE_ZOOKEEPER_QUORUM, BJ_ZK_QUORUM);
    Configuration desConf = new Configuration();
    desConf.set(HBASE_ZOOKEEPER_QUORUM, GZ_ZK_QUORUM);
    
    SyncUidList sul = new SyncUidList();
    try {
      sul.runTool(srcConf, desConf, args);
    } catch (Exception e) {
      LOG.warn("", e);
    }
  }
}
