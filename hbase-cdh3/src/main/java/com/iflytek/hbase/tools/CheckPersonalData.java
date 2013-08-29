package com.iflytek.hbase.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.iflytek.hbase.thrift.generated.Hbase;
import com.iflytek.hbase.thrift.generated.TCell;
import com.iflytek.personal.PathParser;

/**
 * <p>
 * This tool is used for check personal data between old version hbase-cdh3 and
 * new version hbase-cdh4.</br>
 * </p>
 * 
 * @author mingzhang2
 * @version 1.0
 * @see
 */
public class CheckPersonalData {
  private static final Log LOG = LogFactory.getLog(CheckPersonalData.class);
  private static int CHECK_INTERVAL = 10240;
  private static String LINE_END = System.getProperty("line.separator");
  
  private static String baseDir = null;
  private static String outputDir = null;
  private static String outputPrefix = "check_";
  
  private HTablePool tablePool = null;
  
  public class RowMessage {
    String rowKey;
    
  }
  
  public class PersonalDataReport {
    private Map<String,RowMessage> rowMap = new HashMap<String,RowMessage>();
    private ArrayList<String> rowList = new ArrayList<String>();
    
    public void putMsg(String rowKey, RowMessage msg) {
      rowList.add(rowKey);
      rowMap.put(rowKey, msg);
    }
    
    public void writeReport(File file) {
      Collections.sort(rowList);
      PrintWriter pw = null;
      try {
        pw = new PrintWriter(file);
        for (String row : rowList) {
          
          pw.flush();
        }
      } catch (FileNotFoundException e) {
        LOG.warn("", e);
      } finally {
        if (pw != null) {
          pw.close();
        }
      }
    }
  }
  
  public int runTool(Configuration conf, String[] args) {
    tablePool = new HTablePool(conf, 128);
    HTableInterface table = tablePool.getTable("personal");
    
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("cf"));
    ResultScanner scanner = null;
    Result result = null;
    byte[] row = null;
    String rowStr = null;
    
    TTransport transport = new TSocket("192.168.150.22", 9090);
    TProtocol protocol = new TBinaryProtocol(transport);
    Hbase.Client client = new Hbase.Client(protocol);
    Map<ByteBuffer,ByteBuffer> attributes = new HashMap<ByteBuffer,ByteBuffer>();
    
    try {
      transport.open();
      scanner = table.getScanner(scan);
      int tempCount = CHECK_INTERVAL;
      while ((result = scanner.next()) != null && tempCount-- > 0) {
        row = result.getRow();
        rowStr = Bytes.toString(row);
        String newRowStr = rowStr.startsWith("aa") ? rowStr.substring(1) : rowStr;
        Map<byte[],byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("cf"));
        for (Map.Entry<?,?> entry : familyMap.entrySet()) {
          String qualify = Bytes.toString((byte[]) entry.getKey());
          byte[] value = (byte[]) entry.getValue();
          PathParser parser = new PathParser();
          parser.parsePartPath(newRowStr, qualify);
          ByteBuffer newColumn = ByteBuffer.wrap(Bytes.toBytes(parser.column));
          ByteBuffer newTableName = ByteBuffer.wrap(Bytes.toBytes("personal"));
          ByteBuffer newRow = ByteBuffer.wrap(Bytes.toBytes(rowStr
              .startsWith("aa") ? rowStr.substring(1) : rowStr));
          List<TCell> cellList = client.get(newTableName, newRow, newColumn,
              attributes);
          if (cellList.isEmpty()) {
            LOG.info("new hbase does not have this cell, row: " + parser.rowKey
                + ", column: " + parser.column);
          }
        }
      }
      
    } catch (Exception e) {
      LOG.info("", e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    baseDir = System.getProperty("base.dir");
    outputDir = baseDir + "/check_data/";
    File outputDirFile = new File(outputDir);
    if (outputDirFile.exists()) {
      outputDirFile.mkdirs();
    }
    PropertyConfigurator.configure(baseDir + "/../conf/log4j.properties");
    
    LOG.info("start");
    Configuration conf = new Configuration();
    // conf.addResource("hbase-tools.xml");
    conf.set("hbase.zookeeper.quorum", "192.168.150.16,192.169.150.17,"
        + "192.168.150.18,192.168.150.19,192.168.150.20");
    
    CheckPersonalData cpd = new CheckPersonalData();
    cpd.runTool(conf, args);
    LOG.info("stop");
  }
}
