package com.iflytek.hbase;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanTool {
  private static final Log LOG = LogFactory.getLog(ScanTool.class);
  
  private Options options = new Options();
  private CommandLine cmdLine;
  
  private Configuration conf;
  
  public ScanTool() throws Exception {
    options.addOption("n", "scan-number", true, "Number of scan count!");
    options.addOption("o", "output", true, "Output file");
    options.addOption("s", "start-row", true, "Start row");
    options.addOption("h", "help", false, "help");
    options.addOption("t", "table", true, "table name");
  }
  
  public void usage() {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp("scan [-p][-v/--verbose][--block-size][-h/--help]", options);
  }
  
  public void runTool(Configuration conf, String[] args) throws Exception {
    cmdLine = Common.parseOptions(options, args);
    this.conf = new Configuration(conf);
    if (cmdLine.hasOption('h')) {
      usage();
      return;
    }
    
    String tableName = null;
    if (cmdLine.hasOption('t')) {
      tableName = cmdLine.getOptionValue('t');
    } else {
      usage();
      return;
    }
    
    HTable table = new HTable(conf, tableName);
    int scanCount = 10;
    String outputFileName = "output";
    if (cmdLine.hasOption('n')) {
      scanCount = Integer.parseInt(cmdLine.getOptionValue('n'));
    }
    
    if (cmdLine.hasOption('o')) {
      outputFileName = cmdLine.getOptionValue('o');
    }
    
    Scan scan = new Scan();
    if (cmdLine.hasOption('s')) {
      scan.setStartRow(Bytes.toBytes(cmdLine.getOptionValue('s')));
    }
    
    ResultScanner scanner = table.getScanner(scan);
    Result result = null;
    
    StringBuilder sb = new StringBuilder();
    String row = null;
    String column = null;
    String family = null;
    String qualify = null;
    byte[] value = null;
    int valueLength = 0;
    String digest = null;
    NavigableMap<byte[],NavigableMap<byte[],byte[]>> noVersionMap = null;
    NavigableMap<?,?> familyMap = null;
    MessageDigest msgDigest = MessageDigest.getInstance("MD5");
    BigInteger bigInt = null;
    long timestamp = 0;
    KeyValue kv = null;
    String date = null;
    
    while (scanCount-- > 0 && (result = scanner.next()) != null) {
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
          date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(
              timestamp));
          
          value = (byte[]) familyEntry.getValue();
          bigInt = new BigInteger(1, msgDigest.digest(value));
          digest = bigInt.toString(16);
          
          valueLength = value.length;
          sb.append(Common.completionString("", 4));
          sb.append(Common.completionString(family + ":" + qualify, ' ', 64,
              false)
              + Common.completionString("", 4)
              + Common.completionString("" + valueLength, 10)
              + Common.completionString("", 4)
              + date
              + Common.completionString("", 4)
              + Common.completionString(digest, '0', 32, true));
          sb.append(Constants.LINE_SEPARATOR);
        }
      }
    }
    
    File output = new File(outputFileName);
    PrintWriter pw = new PrintWriter(output);
    pw.write(sb.toString());
    pw.close();
    scanner.close();
    table.close();
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Common.globalInit();
    
    Configuration conf = new Configuration();
    conf.addResource("hbase-tools.xml");
    
    ScanTool st = new ScanTool();
    long startTime = System.currentTimeMillis();
    st.runTool(conf, args);
    long endTime = System.currentTimeMillis();
    LOG.info("Total time cost: " + (endTime - startTime) + "ms");
  }
}
