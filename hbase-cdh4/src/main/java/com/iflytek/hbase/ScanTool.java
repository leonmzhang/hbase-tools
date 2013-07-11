package com.iflytek.hbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanTool {
  
  private Options options = new Options();
  private CommandLine cmdLine;
  
  private Configuration conf;
  
  public ScanTool(Configuration conf, String[] args) throws Exception {
    options.addOption("n", "scan-number", true, "Number of scan count!");
    cmdLine = Common.parseOptions(options, args);
    this.conf = new Configuration(conf);
  }
  
  public void runTool() throws Exception {
    HTable table = new HTable(conf, "personal");
    int scanCount = 10; 
    if(cmdLine.hasOption('n')) {
      scanCount = Integer.parseInt(cmdLine.getOptionValue('n'));
    }
    
    Scan scan = new Scan();
    
    ResultScanner scanner = table.getScanner(scan);
    Result result = null;
    while(scanCount-- > 0 && (result = scanner.next()) != null) {
      System.out.println(Bytes.toString(result.getRow()));
    } 
    
    scanner.close();
    table.close();
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Common.globalInit();
    
    Configuration conf = new Configuration();
    
    ScanTool st = new ScanTool(conf, args);
    st.runTool();
  }
}
