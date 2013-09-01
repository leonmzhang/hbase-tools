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
import com.iflytek.personal.Personal;
import com.iflytek.personal.PersonalParseException;
import com.iflytek.personal.PersonalUtil;
import com.iflytek.personal.RowMessage;

public class SyncTable implements Tool {
  /* *
   * these configurations should be in configuration file, but now I put them
   * here for fast implement
   */
  private static final String ZK_QUORUM = "192.168.150.16,192.168.150.17,"
      + "192.192.168.150.18,192.168.150.19,192.168.150.20";
  private static final String THRIFT_SERVERS = "192.168.150.22,"
      + "192.168.150.23,192.168.150.24,192.168.150.25";
  
  private static final Log LOG = LogFactory.getLog(SyncTable.class);
  /* the interval of sync task, 30 minute */
  private static final long SYNC_INTERVAL = 30 * 60 * 1000;
  /* the scan interval, check changes for 40 minute before */
  private static final long SCAN_INTERVAL = 40 * 60 * 1000;
  /* scan threads per task */
  private static final int WORKER_COUNT = PersonalUtil.KEY.length;
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
  
  private AtomicInteger totalCount;
  
  public SyncTable(Configuration conf) {
    this.conf = new Configuration(conf);
    syncTimer = new Timer();
  }
  
  public class SyncTask extends TimerTask {
    
    int taskId = 0;
    
    @Override
    public void run() {
      LOG.info("New sync task " + this.taskId + " is begin.");
      this.taskId = taskID.incrementAndGet();
      Date taskStartTime = new Date();
      long startTimestamp = System.currentTimeMillis();
      
      ExecutorService exec = Executors.newFixedThreadPool(WORKER_COUNT);
      for (int i = 0; i < WORKER_COUNT; i++) {
        LOG.info("start scan worker for key: " + PersonalUtil.KEY[i]);
        exec.execute(new Worker(PersonalUtil.KEY[i]));
      }
      exec.shutdown();
      
      while (!exec.isTerminated()) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {}
      }
      
      long endTimestamp = System.currentTimeMillis();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      LOG.info("Sync task " + this.taskId + " which start at "
          + sdf.format(taskStartTime) + "is done, total cost: "
          + (endTimestamp - startTimestamp) + ".");
    }
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
    
    private String startRow;
    private String endRow;
    
    public Worker(String rowRange) {
      this.rowRange = rowRange;
    }
    
    private void parseRowRange() {
      String[] array = rowRange.split(";");
      this.startRow = array[0];
      this.endRow = array[1];
    }
    
    private void setup() throws TTransportException {
      String host = getDesThriftServer();
      int port = 9090;
      transport = new TSocket(host, port);
      protocol = new TBinaryProtocol(transport);
      client = new Hbase.Client(protocol);
      transport.open();
      
      table = tablePool.getTable(PersonalUtil.OLD_PERSONAL_TABLE);
    }
    
    private void cleanup() {
      try {
        transport.close();
        table.close();
      } catch (IOException e) {
        LOG.warn("worker cleanup failed", e);
      }
    }
    
    private void doWork() {
      parseRowRange();
      
      ResultScanner scanner = null;
      Result result = null;
      StringBuilder sb = null;
      RowMessage srcRowMsg = null;
      RowMessage desRowMsg = null;
      
      NavigableMap<byte[],NavigableMap<byte[],byte[]>> noVersionMap = null;
      NavigableMap<?,?> familyMap = null;
      
      String lastScanRow = startRow;
      
      String row = null;
      String family = null;
      String qualify = null;
      long timestamp = 0;
      byte[] value;
      BigInteger bigInt = null;
      MessageDigest msgDigest = null;
      int valueLength = 0;
      
      int count = 0;
      
      Map<ByteBuffer,ByteBuffer> attributes = new HashMap<ByteBuffer,ByteBuffer>();
      Mutation mutation = null;
      List<Mutation> mutations = null;
      
      long currentTimestamp = System.currentTimeMillis();
      
      Scan scan = new Scan();
      
      try {
        msgDigest = MessageDigest.getInstance("MD5");
        
        scan.setTimeRange(currentTimestamp - SCAN_INTERVAL, currentTimestamp);
        scan.addFamily(Bytes.toBytes("cf"));
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scanner = table.getScanner(scan);
        
        do {
          try {
            result = scanner.next();
            if (result == null) {
              break;
            }
          } catch (ScannerTimeoutException e) {
            LOG.warn("scanner timeout, get scanner from last row: "
                + lastScanRow, e);
            scan.setStartRow(Bytes.toBytes(lastScanRow));
            scanner.close();
            scanner = table.getScanner(scan);
            continue;
          }
          
          sb = new StringBuilder();
          srcRowMsg = new RowMessage();
          desRowMsg = new RowMessage();
          
          row = Bytes.toString(result.getRow());
          lastScanRow = row;
          noVersionMap = result.getNoVersionMap();
          
          for (Map.Entry<?,?> entry : noVersionMap.entrySet()) {
            family = Bytes.toString((byte[]) entry.getKey());
            familyMap = (NavigableMap<?,?>) entry.getValue();
            for (Map.Entry<?,?> familyEntry : familyMap.entrySet()) {
              qualify = Bytes.toString((byte[]) familyEntry.getKey());
              timestamp = result.getColumnLatest(Bytes.toBytes(family),
                  Bytes.toBytes(qualify)).getTimestamp();
              value = (byte[]) familyEntry.getValue();
              bigInt = new BigInteger(1, msgDigest.digest(value));
              valueLength = value.length;
              
              srcRowMsg.appendRow(row);
              srcRowMsg.appendCellMsg(family, qualify, timestamp, valueLength,
                  bigInt);
              
              // new peronsal data format
              Personal personal = new Personal();
              try {
                personal.parsePersonalData(row, family, qualify, value);
                desRowMsg.appendRow(personal.getHbaseCell().getRowKey());
                desRowMsg.appendCellMsg(personal.getHbaseCell().getFamily(),
                    personal.getHbaseCell().getQualify(), timestamp,
                    valueLength, bigInt);
                
                mutations = new ArrayList<Mutation>();
                mutation = new Mutation();
                mutation.setColumn(Bytes.toBytes(personal.getHbaseCell()
                    .getColumn()));
                mutation.setValue(value);
                mutations.add(mutation);
                ByteBuffer newTableName = ByteBuffer.wrap(Bytes
                    .toBytes(personal.getHbaseCell().getTable()));
                ByteBuffer newRow = ByteBuffer.wrap(Bytes.toBytes(personal
                    .getHbaseCell().getRowKey()));
                ByteBuffer newColumn = ByteBuffer.wrap(Bytes.toBytes(personal
                    .getHbaseCell().getColumn()));
                List<TCell> cellList = client.get(newTableName, newRow,
                    newColumn, attributes);
                if (cellList.isEmpty()) {
                  client.mutateRowTs(newTableName, newRow, mutations,
                      timestamp, attributes);
                } else if (cellList.get(0).timestamp < timestamp) {
                  client.mutateRowTs(newTableName, newRow, mutations,
                      timestamp, attributes);
                }
              } catch (PersonalParseException e) {
                LOG.warn("", e);
              } catch (Exception e) {
                LOG.warn("", e);
              }
            }
          }
          
          sb.append(srcRowMsg.getMsg());
          sb.append(desRowMsg.getMsg());
          sb.append(Constants.LINE_SEPARATOR);
          printQueue.put(sb);
          count = totalCount.incrementAndGet();
          if (count % 1000 == 0) {
            LOG.info("Already scan: " + count);
          }
        } while (result != null);
      } catch (Exception e) {
        LOG.warn("", e);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
      cleanup();
    }
    
    @Override
    public void run() {
      try {
        setup();
        doWork();
      } catch (TTransportException e) {
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
    totalCount = new AtomicInteger();
    
    //String thriftServerStr = conf.get(Constants.DES_HBASE_THRIFT_SERVERS);
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
    
    /**
     * 启动timer
     */
    syncTimer.schedule(new SyncTask(), 0, SYNC_INTERVAL);
    cleanup();
    return 0;
    
    /* ExecutorService printExec = Executors.newSingleThreadExecutor();
    printExec.execute(new Printer());
    printExec.shutdown();
    
    int threadPoolSize = PersonalUtil.KEY.length;
    ExecutorService exec = Executors.newFixedThreadPool(threadPoolSize);
    for (int i = 0; i < threadPoolSize; i++) {
      exec.execute(new Worker(PersonalUtil.KEY[i]));
    }
    exec.shutdown();
    while (!exec.isTerminated()) {
      Thread.sleep(500);
    }
    printSignal = false; */
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    //Common.globalInit();
    baseDir = System.getProperty("base.dir");
    PropertyConfigurator.configure(baseDir + "/conf/log4j.properties");
    
    Configuration conf = new Configuration();
    //conf.addResource("hbase-tools.xml");
    
    SyncTable st = new SyncTable(conf);
    ToolRunner.run(st, args);
  }
}
