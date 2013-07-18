package com.iflytek.hbase;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
  private static final Log LOG = LogFactory.getLog(SyncTable.class);
  
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
    private String key;
    private TTransport transport;
    private TProtocol protocol;
    private Hbase.Client client;
    private HTableInterface table;
    
    public Worker(String key) {
      this.key = key;
    }
    
    private void setup() throws TTransportException {
      String host = getDesThriftServer();
      int port = 9090;
      transport = new TSocket(host, port);
      protocol = new TBinaryProtocol(transport);
      client = new Hbase.Client(protocol);
      transport.open();
      
      table = tablePool.getTable("personal");
    }
    
    private void cleanup() {
      transport.close();
      try {
        table.close();
      } catch (IOException e) {
        LOG.warn("", e);
      }
    }
    
    @Override
    public void run() {
      try {
        setup();
      } catch (TTransportException e) {
        LOG.warn("", e);
        cleanup();
        return;
      }
      
      Scan scan = new Scan();
      String minDateStr = conf.get(Constants.SCAN_MIN_DATE);
      String maxDateStr = conf.get(Constants.SCAN_MAX_DATE);
      long minStamp = Common.dateStrToUnixTimestamp(minDateStr);
      long maxStamp = Common.dateStrToUnixTimestamp(maxDateStr);
      ResultScanner scanner = null;
      Result result = null;
      StringBuilder sb = null;
      RowMessage srcRowMsg = null;
      RowMessage desRowMsg = null;
      
      NavigableMap<byte[],NavigableMap<byte[],byte[]>> noVersionMap = null;
      NavigableMap<?,?> familyMap = null;
      
      String row = null;
      String family = null;
      String qualify = null;
      long timestamp = 0;
      byte[] value;
      BigInteger bigInt = null;
      MessageDigest msgDigest = null;
      int valueLength = 0;
      
      String[] array = key.split(";");
      String startRow = array[0];
      String endRow = array[1];
      
      int count = 0;
      
      Map<ByteBuffer,ByteBuffer> attributes = new HashMap<ByteBuffer,ByteBuffer>();
      Mutation mutation = null;
      List<Mutation> mutations = null;
      
      try {
        msgDigest = MessageDigest.getInstance("MD5");
        scan.setTimeRange(minStamp, maxStamp);
        scan.addFamily(Bytes.toBytes("cf"));
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scanner = table.getScanner(scan);
        
        while ((result = scanner.next()) != null) {
          sb = new StringBuilder();
          srcRowMsg = new RowMessage();
          desRowMsg = new RowMessage();
          
          row = Bytes.toString(result.getRow());
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
        }
      } catch (Exception e) {
        LOG.warn("", e);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
      cleanup();
    }
  }
  
  private void setup(String[] args) throws Exception {
    conf.set(Constants.HBASE_ZOOKEEPER_QUORUM,
        conf.get(Constants.SRC_HBASE_ZOOKEEPER_QUORUM));
    tablePool = new HTablePool(conf, 1024);
    printQueue = new LinkedBlockingDeque<StringBuilder>();
    printSignal = true;
    totalCount = new AtomicInteger();
    
    String thriftServerStr = conf.get(Constants.DES_HBASE_THRIFT_SERVERS);
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
    
    ExecutorService printExec = Executors.newSingleThreadExecutor();
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
    printSignal = false;
    
    cleanup();
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Common.globalInit();
    
    Configuration conf = new Configuration();
    conf.addResource("hbase-tools.xml");
    
    SyncTable st = new SyncTable(conf);
    long startTime = System.currentTimeMillis();
    ToolRunner.run(st, args);
    long stopTime = System.currentTimeMillis();
    LOG.info("Total time cost: " + (stopTime - startTime) + "ms");
  }
}
