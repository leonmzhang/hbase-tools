package com.iflytek.hbase.main;

import java.io.IOException;
import java.util.ArrayList;

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
    Path pathApp = new Path("/msp/app");
    Path pathGws = new Path("/msp_gws");
    
    ArrayList<Path> fileList = getFileList(conf, pathApp);
    
    return 0;
  }
  
  private ArrayList<Path> getFileList(Configuration conf, Path dirPath) {
    ArrayList<Path> fileList = new ArrayList<Path>();
    try {
      FileSystem fs = FileSystem.get(conf);
      
      FileStatus[] statusArray = fs.listStatus(dirPath);
      for (FileStatus status : statusArray) {
        LOG.info(status.getPath().toString());
      }
      
    } catch (IOException e) {
      LOG.warn("", e);
    }
    
    return null;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    Common.globalInit();
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://namenode-gz.iflytek.com:9040");
    HdfsToHbase h2h = new HdfsToHbase();
    h2h.runTool(conf, args);
  }
  
}
