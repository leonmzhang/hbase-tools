package com.iflytek.hbase;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
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

import com.iflytek.personal.PersonalUtil;

public class SyncTable implements Tool {
  private static final Log LOG = LogFactory.getLog(SyncTable.class);
  private Configuration conf;
  private Configuration srcConf;
  private Configuration desConf;
  private Options options = new Options();
  private CommandLine cmdLine;
  
  private HTablePool tablePool;
  
  private BlockingQueue<StringBuilder> printQueue;
  private boolean printSignal;
  
  public SyncTable(Configuration conf) {
    this.conf = new Configuration(conf);
    this.srcConf = new Configuration(conf);
    this.desConf = new Configuration();
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
            pw.write(sb.toString() + Constants.LINE_SEPARATOR);
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
    private Configuration conf;
    private Configuration srcConf;
    private String key;
    
    public Worker(Configuration conf, String key) {
      this.conf = new Configuration(conf);
      this.srcConf = new Configuration(conf);
      this.key = key;
    }
    
    @Override
    public void run() {
      HTableInterface table = tablePool.getTable("personal");
      
      Scan scan = new Scan();
      String minDateStr = conf.get(Constants.SCAN_MIN_DATE);
      String maxDateStr = conf.get(Constants.SCAN_MAX_DATE);
      long minStamp = Common.dateStrToUnixTimestamp(minDateStr);
      long maxStamp = Common.dateStrToUnixTimestamp(maxDateStr);
      ResultScanner scanner = null;
      Result result = null;
      StringBuilder sb = null;
      
      NavigableMap<byte[],NavigableMap<byte[],byte[]>> noVersionMap = null;
      NavigableMap<?,?> familyMap = null;
      
      String row = null;
      String family = null;
      String qualify = null;
      long timestamp = 0;
      String date = null;
      byte[] value;
      BigInteger bigInt = null;
      String digest = null;
      MessageDigest msgDigest = null;
      int valueLength = 0;
      
      String[] array = key.split(";");
      String startRow = array[0];
      String endRow = array[1];
      
      try {
        msgDigest = MessageDigest.getInstance("MD5");
        scan.setTimeRange(minStamp, maxStamp);
        scan.addFamily(Bytes.toBytes("cf"));
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scanner = table.getScanner(scan);
        while ((result = scanner.next()) != null) {
          sb = new StringBuilder();
          // ---
          row = Bytes.toString(result.getRow());
          sb.append(row);
          sb.append(Constants.LINE_SEPARATOR);
          noVersionMap = result.getNoVersionMap();
          
          for (Map.Entry<?,?> entry : noVersionMap.entrySet()) {
            family = Bytes.toString((byte[]) entry.getKey());
            familyMap = (NavigableMap<?,?>) entry.getValue();
            for (Map.Entry<?,?> familyEntry : familyMap.entrySet()) {
              qualify = Bytes.toString((byte[]) familyEntry.getKey());
              timestamp = result.getColumnLatest(Bytes.toBytes(family),
                  Bytes.toBytes(qualify)).getTimestamp();
              date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  .format(new Date(timestamp));
              
              value = (byte[]) familyEntry.getValue();
              bigInt = new BigInteger(1, msgDigest.digest(value));
              digest = bigInt.toString(16);
              
              valueLength = value.length;
              sb.append(Common.completionString("", 4));
              sb.append(Common.completionString(family + ":" + qualify, ' ',
                  64, false)
                  + Common.completionString("", 4)
                  + Common.completionString("" + valueLength, 10)
                  + Common.completionString("", 4)
                  + date
                  + Common.completionString("", 4)
                  + Common.completionString(digest, '0', 32, true));
              sb.append(Constants.LINE_SEPARATOR);
            }
          }
          printQueue.put(sb);
        }
      } catch (IOException e) {
        LOG.warn("", e);
      } catch (NoSuchAlgorithmException e) {
        LOG.warn("", e);
      } catch (InterruptedException e) {
        LOG.warn("", e);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
        try {
          table.close();
        } catch (IOException e) {
          LOG.warn("", e);
        }
      }
    }
  }
  
  private void setup(String[] args) throws Exception {
    options.addOption("n", "number", true, "number of scan");
    tablePool = new HTablePool(srcConf, 1024);
    cmdLine = Common.parseOptions(options, args);
    printQueue = new LinkedBlockingDeque<StringBuilder>();
    printSignal = true;
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
      exec.execute(new Worker(conf, PersonalUtil.KEY[i]));
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
    ToolRunner.run(st, args);
  }
}
