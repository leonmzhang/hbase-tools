package com.iflytek.hbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetTool implements Tool {
  private Configuration conf = null;
  private CommandLine cmdLine = null;
  private Options options = new Options();
  
  public GetTool(Configuration conf) {
    setConf(conf);
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void setConf(Configuration conf) {
    conf = new Configuration(conf);
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
    if (cmdLine.hasOption('t')) {
      tableName = cmdLine.getOptionValue('t');
    } else {
      usage();
      return -1;
    }
    
    HTable table = new HTable(conf, tableName);
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.addResource("hbase-tools.xml");
    ToolRunner.run(new GetTool(conf), args);
  }
}
