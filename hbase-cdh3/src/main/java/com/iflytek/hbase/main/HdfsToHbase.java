package com.iflytek.hbase.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;

import com.iflytek.hbase.Common;

public class HdfsToHbase {
  private static final Log LOG = LogFactory.getLog(HdfsToHbase.class);
  
  public int runTool(Configuration conf, String[] args) {
    Path pathApp = new Path("/msp/app");
    Path pathGws = new Path("/msp_gws");
    
    try {
      ArrayList<Path> fileList = getFileList(conf, pathApp);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return 0;
  }
  
  private ArrayList<Path> getFileList(Configuration conf, Path dirPath)
      throws Exception {
    ArrayList<Path> fileList = new ArrayList<Path>();
    ArrayList<Path> dirPathArray = new ArrayList<Path>();
    dirPathArray.add(dirPath);
    
    Path tempPath = null;
    FileStatus[] statusArray = null;
    FileSystem fs = FileSystem.get(conf);
    
    do {
      tempPath = dirPathArray.remove(0);
      statusArray = fs.listStatus(tempPath);
      for (FileStatus status : statusArray) {
        if (status.isDir()) {
          dirPathArray.add(status.getPath());
        } else {
          fileList.add(status.getPath());
          LOG.info(status.getPath().toString());
        }
      }
    } while (dirPathArray.size() != 0);
    
    return fileList;
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
