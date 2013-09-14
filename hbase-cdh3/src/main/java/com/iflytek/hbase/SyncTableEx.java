package com.iflytek.hbase;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.iflytek.hbase.thrift.generated.Hbase;
import com.iflytek.hbase.thrift.generated.Mutation;
import com.iflytek.hbase.thrift.generated.TCell;
import com.iflytek.personal.PersonalCell;
import com.iflytek.personal.PersonalParseException;
import com.iflytek.personal.PersonalUtil;
import com.iflytek.personal.RowMessage;

public class SyncTableEx implements Tool {
  /* *
   * these configurations should be in configuration file, but now I put them
   * here for fast implement
   */
  // gz
  // private static final String ZK_QUORUM = "192.168.150.16,192.168.150.17,"
  // + "192.168.150.18,192.168.150.19,192.168.150.20";
  private static final String THRIFT_SERVERS = "192.168.150.22,"
      + "192.168.150.23,192.168.150.24,192.168.150.25";
  
  // bj
  // private static final String ZK_QUORUM = "192.168.71.47,192.168.71.239,"
  // +
  // "192.168.71.241,192.168.71.243,192.168.71.245,192.168.71.248,192.168.71.249";
  // private static final String THRIFT_SERVERS =
  // "192.168.151.103,192.168.151.104,"
  // +
  // "192.168.151.104,192.168.151.106,192.168.151.107,192.168.151.108,192.168.151.109,"
  // + "192.168.151.110";
  
  // hf
  private static final String ZK_QUORUM = "192.168.52.13,192.168.52.14,"
      + "192.168.52.15,192.168.52.16,192.168.52.17";
  // private static final String THRIFT_SERVERS =
  // "192.168.52.232,192.168.52.233,"
  // + "192.168.52.234,192.168.52.248";
  
  private static final Log LOG = LogFactory.getLog(SyncTable.class);
  /* the interval of sync task, 30 minute */
  private static final long SYNC_INTERVAL = 30 * 60 * 1000;
  /* the scan interval, check changes for 40 minute before */
  private static final long SCAN_INTERVAL = 40 * 60 * 1000;
  /* scan threads per task */
  private static final int WORKER_COUNT = PersonalUtil.KEY.length;
  /* 2012-01-01 00:00:00 */
  private long THE_VERY_BEGINNING = 1325347200000L;
  /* 2013-09-06 00:00:00 */
  private long THE_NEW_DEPLOY_TIME_GZ = 1378396800000L;
  /* 2013-09-12 00:00:00 */
  private long THE_NEW_DEPLOY_TIME_BJ = 1378915200000L;
  /* the base dir defined by base.dir property */
  public static String baseDir = "";
  /* an incremental id */
  private AtomicInteger taskID = new AtomicInteger(0);
  
  private Timer syncTimer;
  
  private ArrayList<String> thriftServers = new ArrayList<String>();
  
  private String getDesThriftServer() {
    int index = (int) (Math.random() * (float) thriftServers.size());
    return thriftServers.get(index);
  }
  
  private Configuration conf;
  private HTablePool tablePool;
  private BlockingQueue<StringBuilder> printQueue;
  private boolean printSignal;
  
  private AtomicInteger taskSyncCount = new AtomicInteger(0);
  private AtomicInteger firstSyncCount = new AtomicInteger(0);
  
  public SyncTableEx(Configuration conf) {
    this.conf = new Configuration(conf);
    syncTimer = new Timer();
  }
  
  public class Printer implements Runnable {
    
    @Override
    public void run() {
      PrintWriter pw = null;
      StringBuilder sb = null;
      try {
        pw = new PrintWriter(new File("sync_output"));
        while (printSignal) {
          sb = null;
          try {
            sb = printQueue.poll(1000, TimeUnit.MILLISECONDS);
            if (sb == null) {
              continue;
            }
            pw.write(sb.toString());
            pw.flush();
          } catch (InterruptedException e) {
            LOG.warn("", e);
          }
        }
      } catch (Exception e) {
        LOG.warn("", e);
        return;
      } finally {
        if (pw != null) {
          pw.close();
        }
      }
    }
  }
  
  public class Worker implements Runnable {
    private String rowRange;
    private TTransport transport;
    private TProtocol protocol;
    private Hbase.Client client;
    private HTableInterface table;
    
    private long startTime;
    private long endTime;
    
    private String startRow;
    private String endRow;
    
    private boolean firstSyncFlag = false;
    
    public Worker(String rowRange, boolean firstSync) {
      this.rowRange = rowRange;
      this.firstSyncFlag = firstSync;
      firstSyncFlag = firstSync;
      startTime = THE_VERY_BEGINNING;
      // endTime = System.currentTimeMillis();
      endTime = THE_NEW_DEPLOY_TIME_GZ;
    }
    
    public Worker(String rowRange) {
      this.rowRange = rowRange;
      firstSyncFlag = false;
      startTime = System.currentTimeMillis() - SCAN_INTERVAL;
      endTime = System.currentTimeMillis();
    }
    
    private void parseRowRange() {
      String[] array = rowRange.split(";");
      this.startRow = array[0];
      this.endRow = array[1];
    }
    
    private void setup() throws Exception {
      String host = getDesThriftServer();
      int port = 9090;
      transport = new TSocket(host, port);
      protocol = new TBinaryProtocol(transport);
      client = new Hbase.Client(protocol);
      transport.open();
      
      table = tablePool.getTable(PersonalUtil.OLD_PERSONAL_TABLE);
      if (table == null) {
        String msg = "get HTable instance failed!";
        LOG.warn(msg);
        throw new Exception(msg);
      }
    }
    
    private void cleanup() {
      try {
        transport.close();
        if (table != null) {
          table.close();
        }
      } catch (IOException e) {
        LOG.warn("worker cleanup failed", e);
      }
    }
    
    private void doWork() {
      parseRowRange();
      
      String lastScanRow = startRow;
      
      BigInteger bigInt = null;
      MessageDigest msgDigest = null;
      int valueLength = 0;
      
      /* use for new personal table */
      Map<ByteBuffer,ByteBuffer> attributes = new HashMap<ByteBuffer,ByteBuffer>();
      List<Mutation> mutations = new ArrayList<Mutation>();
      Mutation mutation = null;
      
      /* use to scan old personal table */
      ResultScanner scanner = null;
      Scan scan = new Scan();
      Result result = null;
      NavigableMap<?,?> familyMap = null;
      
      /* these var is old cell info */
      String oldRowKey = null;
      String oldQualify = null;
      long oldTimestamp = 0;
      byte[] value;
      
      /* */
      String newTable = null;
      String newRowKey = null;
      String newColumn = null;
      ByteBuffer newTableByte = null;
      ByteBuffer newRowByte = null;
      ByteBuffer newColumnByte = null;
      
      try {
        msgDigest = MessageDigest.getInstance("MD5");
        
        scan.setTimeRange(startTime, endTime);
        LOG.info("set time range, min time: "
            + Common.unixTimestampToDateStr(startTime) + ", max time: "
            + Common.unixTimestampToDateStr(endTime));
        scan.addFamily(PersonalUtil.OLD_FAMILY_BYTE);
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scanner = table.getScanner(scan);
        
        do {
          try {
            result = scanner.next();
            if (result == null) {
              break;
            }
          } catch (Exception e) {
            LOG.warn("scanner exception, get scanner from last row: "
                + lastScanRow, e);
            scan.setStartRow(Bytes.toBytes(lastScanRow));
            scanner.close();
            scanner = table.getScanner(scan);
            continue;
          }
          
          oldRowKey = Bytes.toString(result.getRow());
          lastScanRow = oldRowKey;
          familyMap = result.getFamilyMap(PersonalUtil.OLD_FAMILY_BYTE);
          
          for (Map.Entry<?,?> entry : familyMap.entrySet()) {
            oldQualify = Bytes.toString((byte[]) entry.getKey());
            oldTimestamp = result.getColumnLatest(PersonalUtil.OLD_FAMILY_BYTE,
                Bytes.toBytes(oldQualify)).getTimestamp();
            value = (byte[]) entry.getValue();
            LOG.info((firstSyncFlag ? "first sync, " : "")
                + "get old personal cell, row: " + oldRowKey + ", column: "
                + PersonalUtil.OLD_FAMILY_STR + ":" + oldQualify
                + ", modify time: "
                + Common.unixTimestampToDateStr(oldTimestamp)
                + ", old path: " + oldQualify);
            
            PersonalCell personal = new PersonalCell();
            try {
              personal.parsePersonalData(oldRowKey,
                  PersonalUtil.OLD_FAMILY_STR, oldQualify, value);
              newTable = personal.getHbaseCell().getTable();
              newRowKey = personal.getHbaseCell().getRowKey();
              newColumn = personal.getHbaseCell().getColumn();
              
              mutations.clear();
              mutation = new Mutation();
              mutation.setColumn(Bytes.toBytes(personal.getHbaseCell()
                  .getColumn()));
              mutation.setValue(value);
              mutations.add(mutation);
              
              newTableByte = ByteBuffer.wrap(Bytes.toBytes(newTable));
              newRowByte = ByteBuffer.wrap(Bytes.toBytes(newRowKey));
              newColumnByte = ByteBuffer.wrap(Bytes.toBytes(newColumn));
              
              List<TCell> cellList = client.get(newTableByte, newRowByte,
                  newColumnByte, attributes);
              if (cellList.isEmpty()
                  || cellList.get(0).timestamp < oldTimestamp) {
                LOG.info((firstSyncFlag ? "first sync, " : "")
                    + "sync cell, table: " + newTable + ", row: " + newRowKey
                    + ", column: " + newColumn + ", modify time: "
                    + Common.unixTimestampToDateStr(oldTimestamp));
                client.mutateRowTs(newTableByte, newRowByte, mutations,
                    oldTimestamp, attributes);
              } else {
                LOG.info("do not need to sync.");
              }
            } catch (PersonalParseException e) {
              LOG.warn("", e);
            } catch (Exception e) {
              LOG.warn("", e);
            }
          }
          
          int count = 0;
          if (firstSyncFlag) {
            count = firstSyncCount.incrementAndGet();
            if (count % 1000 == 0) {
              LOG.info("first sync, alread scan: " + count + " rows");
            }
            
          } else {
            count = taskSyncCount.incrementAndGet();
            if (count % 1000 == 0) {
              LOG.info("task sync, alread scan: " + count + " rows");
            }
          }
        } while (result != null);
      } catch (Exception e) {
        LOG.warn("", e);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    }
    
    @Override
    public void run() {
      try {
        setup();
        doWork();
      } catch (Exception e) {
        LOG.error("worker setup failed", e);
        return;
      } finally {
        cleanup();
      }
    }
  }
  
  private void setup(String[] args) throws Exception {
    conf.set(Constants.HBASE_ZOOKEEPER_QUORUM, ZK_QUORUM);
    tablePool = new HTablePool(conf, 1024);
    printQueue = new LinkedBlockingDeque<StringBuilder>();
    printSignal = true;
    
    // String thriftServerStr = conf.get(Constants.DES_HBASE_THRIFT_SERVERS);
    String thriftServerStr = THRIFT_SERVERS;
    String[] thriftServerArray = thriftServerStr.split(",");
    for (String server : thriftServerArray) {
      thriftServers.add(server);
    }
  }
  
  private void cleanup() throws Exception {
    if (tablePool != null) {
      tablePool.close();
    }
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    setup(args);
    
    ExecutorService exec = Executors.newFixedThreadPool(WORKER_COUNT);
    for (int i = 0; i < WORKER_COUNT; i++) {
      LOG.info("start first sync scan worker for row range: "
          + PersonalUtil.KEY[i]);
      exec.execute(new Worker(PersonalUtil.KEY[i], true));
    }
    exec.shutdown();
    
    cleanup();
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    // Common.globalInit();
    baseDir = System.getProperty("base.dir");
    PropertyConfigurator.configure(baseDir + "/conf/log4j.properties");
    
    Configuration conf = new Configuration();
    // conf.addResource("hbase-tools.xml");
    
    SyncTableEx st = new SyncTableEx(conf);
    ToolRunner.run(st, args);
  }
}
