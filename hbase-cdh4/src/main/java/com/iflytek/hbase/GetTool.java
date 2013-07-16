package com.iflytek.hbase;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetTool implements Tool {
  
  private Configuration conf = null;
  private CommandLine cmdLine = null;
  private Options options = new Options();
  
  public GetTool(Configuration conf) {
    setConf(conf);
    options.addOption("r", "row", true, "row key");
    options.addOption("o", "output", true, "Output file");
    options.addOption("h", "help", false, "help");
    options.addOption("t", "table", true, "table name");
    options.addOption("c", "column", true, "column, format: family:qualify");
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
  }
  
  public void usage() {
    
  }
  
  @Override
  public int run(String[] args) throws Exception {
    cmdLine = Common.parseOptions(options, args);
    if (cmdLine.hasOption('h')) {
      usage();
      return -1;
    }
    
    String tableName = null;
    String row = null;
    String column = null;
    String family = null;
    String qualify = null;
    String outputFileName = "output";
    
    if (cmdLine.hasOption('t')) {
      tableName = cmdLine.getOptionValue('t');
    } else {
      usage();
      return -1;
    }
    if (cmdLine.hasOption('r')) {
      row = cmdLine.getOptionValue('r');
    } else {
      usage();
      return -1;
    }
    
    if (cmdLine.hasOption('c')) {
      column = cmdLine.getOptionValue('c');
      String[] strArray = column.split(":", 2);
      if (strArray == null || strArray.length != 2) {
        return -1;
      }
      family = strArray[0];
      qualify = strArray[1];
    } else {
      usage();
      return -1;
    }
    
    if (cmdLine.hasOption('o')) {
      outputFileName = cmdLine.getOptionValue('o');
    }
    
    HTable table = new HTable(conf, tableName);

    Get get = new Get(Bytes.toBytes(row));
    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualify));
    Result result = table.get(get);
    
    byte[] value = result.getValue(Bytes.toBytes(family),
        Bytes.toBytes(qualify));
    FileOutputStream fos = new FileOutputStream(new File(outputFileName));
    fos.write(value);
    fos.close();
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
    GetTool gt = new GetTool(conf);
    ToolRunner.run(gt, args);
  }
}
