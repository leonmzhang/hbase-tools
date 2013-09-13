package com.iflytek.hbase;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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
    File binFile = new File(
        "/home/mingzhang2/hbase-tools/hbase-cdh4/app_4f58bb41@reslist.bin");
    File txtFile = new File(
        "/home/mingzhang2/hbase-tools/hbase-cdh4/app_4f58bb41@reslist.txt");
    
    if (!binFile.exists() || !txtFile.exists()) {
      LOG.warn("files do not exist");
      return -1;
    }
    FileInputStream fisBin = new FileInputStream(binFile);
    FileInputStream fisTxt = new FileInputStream(txtFile);
    
    byte[] binValue = new byte[(int) binFile.length()];
    byte[] txtValue = new byte[(int) txtFile.length()];
    fisBin.read(binValue);
    fisTxt.read(txtValue);
    
    conf.set("hbase.zookeeper.quorum", "192.168.150.21");
    HTable table = new HTable(conf, "personal");
    Put putBin = new Put(Bytes.toBytes("app_4f58bb41"));
    putBin.add(Bytes.toBytes("p"), Bytes.toBytes("reslist.bin"), binValue);
    Put putTxt = new Put(Bytes.toBytes("app_4f58bb41"));
    putTxt.add(Bytes.toBytes("p"), Bytes.toBytes("reslist.txt"), txtValue);
    
    table.put(putBin);
    table.put(putTxt);
    
    table.close();
    
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
