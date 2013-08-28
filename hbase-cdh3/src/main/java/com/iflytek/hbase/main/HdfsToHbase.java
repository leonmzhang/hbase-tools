package com.iflytek.hbase.main;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import com.iflytek.hbase.Common;
import com.iflytek.hbase.thrift.generated.Hbase;
import com.iflytek.hbase.thrift.generated.Mutation;
import com.iflytek.hbase.util.HbaseCell;

public class HdfsToHbase {
  private static final Log LOG = LogFactory.getLog(HdfsToHbase.class);
  
  public class PathInfo {
    public Path path;
    public long timestamp;
  }
  
  public int runTool(Configuration conf, String[] args) {
    Path pathApp = new Path("/msp/app");
    Path pathGws = new Path("/msp_gws");
    
    try {
      ArrayList<PathInfo> appFileList = getFileList(conf, pathApp);
      ArrayList<PathInfo> gwsFileList = getFileList(conf, pathGws);
      
      HbaseCell cell = null;
      
      for(PathInfo pathInfo : appFileList) {
        cell = parseAppPath(pathInfo);
        hbasePut(cell);
      }
      for(PathInfo pathInfo : gwsFileList) {
        cell = parseGwsPath(pathInfo);
        hbasePut(cell);
      }
    } catch (Exception e) {
      LOG.warn("", e);
    }
    
    return 0;
  }
  
  // /msp/app/4e295329/app_4e295329@reslist.bin
  // table: personal
  // row: app_4e295329
  // column: p:reslist.bin
  private HbaseCell parseAppPath(PathInfo pathInfo) {
    HbaseCell cell = new HbaseCell();
    String pathStr = pathInfo.path.toString();
    int index = pathStr.indexOf("9040");
    pathStr = pathStr.substring(index + 4);
    String[] strArray = pathStr.split("/");
    
    return cell;
  }
  
  // /msp_gws/2013-08-22_panguso_hotwords.txt
  // table: personal_other
  // row: /msp_gws/2013-08-22_panguso_hotwords.txt
  // column: p:file
  private HbaseCell parseGwsPath(PathInfo pathInfo) {
    HbaseCell cell = new HbaseCell();
    String pathStr = pathInfo.path.toString();
    int index = pathStr.indexOf("9040");
    pathStr = pathStr.substring(index + 4);
    cell.setRowKey(pathStr);
    cell.setFamily("p");
    cell.setQualify("file");
    cell.setTable("personal_other");
    cell.setTimestamp(pathInfo.timestamp);
    return cell;
  }
  
  private void hbasePut(HbaseCell cell) {
    List<Mutation> mutations = null;
    Mutation mutation = null;
    Hbase.Client client = null;
    Map<ByteBuffer,ByteBuffer> attributes = new HashMap<ByteBuffer,ByteBuffer>();
    long timestamp = 0;
    
    mutations = new ArrayList<Mutation>();
    mutation = new Mutation();
    mutation.setColumn(Bytes.toBytes(""));
    mutation.setValue(cell.getValue());
    mutations.add(mutation);
    ByteBuffer newTableName = ByteBuffer.wrap(Bytes.toBytes(""));
    ByteBuffer newRow = ByteBuffer.wrap(Bytes.toBytes(""));
    ByteBuffer newColumn = ByteBuffer.wrap(Bytes.toBytes(""));
    
    try {
      //client.mutateRowTs(newTableName, newRow, mutations, timestamp, attributes);
    } catch (Exception e) {
      LOG.warn("", e);
    }
  }
  
  private ArrayList<PathInfo> getFileList(Configuration conf, Path dirPath)
      throws Exception {
    ArrayList<PathInfo> fileList = new ArrayList<PathInfo>();
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
          PathInfo pathInfo = new PathInfo();
          pathInfo.path = status.getPath();
          pathInfo.timestamp = status.getModificationTime();
          fileList.add(pathInfo);
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
