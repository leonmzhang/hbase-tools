package com.iflytek.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PutTool implements Tool {
  private static final Log LOG = LogFactory.getLog(GetTool.class);
  
  private Configuration conf = null;
  
  public PutTool(Configuration conf) {
    setConf(conf);
  }
  
  @Override
  public Configuration getConf() {
    
    return null;
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Common.globalInit();
    Configuration conf = new Configuration();
    PutTool pt = new PutTool(conf);
    ToolRunner.run(pt, args);
  }
}
