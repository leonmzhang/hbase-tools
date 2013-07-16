package com.iflytek.hbase;

import java.io.File;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SyncTable implements Tool {
  private static final Log LOG = LogFactory.getLog(SyncTable.class);
  private Configuration conf;
  
  public SyncTable(Configuration conf) {
    setConf(conf);
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
    Scan scan = new Scan();
    String minDateStr = conf.get(Constants.SCAN_MIN_DATE);
    String maxDateStr = conf.get(Constants.SCAN_MAX_DATE);
    long minStamp = Common.dateStrToUnixTimestamp(minDateStr);
    long maxStamp = Common.dateStrToUnixTimestamp(maxDateStr);
    scan.setTimeRange(minStamp, maxStamp);
    scan.addFamily(Bytes.toBytes("cf"));
    
    HTable table = new HTable(conf, "personal");
    ResultScanner scanner = table.getScanner(scan);
    Result result = null;
    
    StringBuilder sb = new StringBuilder();
    while((result = scanner.next()) != null) {
      sb.append(Bytes.toString(result.getRow()));
      sb.append(Constants.LINE_SEPARATOR);
    }
    
    File output = new File("output");
    PrintWriter pw = new PrintWriter(output);
    pw.write(sb.toString());
    pw.close();
    scanner.close();
    table.close();
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
