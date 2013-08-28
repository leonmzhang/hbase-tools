package com.iflytek.hbase.main;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.iflytek.hbase.Common;

public class HdfsToHbase {
  private static final Log LOG = LogFactory.getLog(HdfsToHbase.class);
  
  public int runTool(Configuration conf, String[] args) {
    try {
      FileSystem fs = FileSystem.get(conf);
      Path path_app = new Path("/msp/app");
      Path path_gws = new Path("/msp_gws");
      FileStatus[] statusArray = fs.listStatus(path_app);
      for(FileStatus status : statusArray) {
        LOG.info(status.getPath().getName());
      }
      
    } catch (IOException e) {
      LOG.warn("", e);
    }
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    Common.globalInit();
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://namenode-gz.iflytek.com:9040");
    
  }
  
}
