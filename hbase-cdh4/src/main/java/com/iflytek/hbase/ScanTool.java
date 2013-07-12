package com.iflytek.hbase;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import javax.swing.text.TabableView;

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
  
  public ScanTool() throws Exception {
    options.addOption("n", "scan-number", true, "Number of scan count!");
    options.addOption("o", "output", true, "Output file");
    options.addOption("s", "start-row", true, "Start row");
    options.addOption("h", "help", false, "help");
  }
  
  private void usage() {
    
  }
  
  public void runTool(Configuration conf, String[] args) throws Exception {
    cmdLine = Common.parseOptions(options, args);
    this.conf = new Configuration(conf);
    if (cmdLine.hasOption('h')) {
      usage();
      return;
    }
    
    HTable table = new HTable(conf, "personal");
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
    String faimly = null;
    String qualify = null;
    
    int valueLength = 0;
    NavigableMap<byte[],NavigableMap<byte[],byte[]>> noVersionMap = null;
    NavigableMap<?,?> familyMap = null;
    
    while (scanCount-- > 0 && (result = scanner.next()) != null) {
      row = Bytes.toString(result.getRow());
      sb.append(row);
      sb.append(Constants.LINE_SEPARATOR);
      
      noVersionMap = result.getNoVersionMap();
      
      for (Map.Entry<?,?> entry : noVersionMap.entrySet()) {
        faimly = Bytes.toString((byte[]) entry.getKey());
        familyMap = (NavigableMap<?,?>) entry.getValue();
        for (Map.Entry<?,?> familyEntry : familyMap.entrySet()) {
          qualify = Bytes.toString((byte[]) familyEntry.getKey());
          valueLength = ((byte[]) familyEntry.getValue()).length;
          sb.append(Constants.TAB);
          sb.append(faimly + ":" + qualify + Constants.TAB + valueLength);
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
    st.runTool(conf, args);
  }
}
