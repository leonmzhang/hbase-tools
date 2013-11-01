package com.iflytek.hbase.demand;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

public class ExportNlp {
  private static final Log LOG = LogFactory.getLog(ExportNlp.class);
  private static String baseDir = "";
  private static File outputDir = null;
  
  public int runTool(Configuration conf, String[] args) {
    try {
      HTable table = new HTable(conf, "personal");
      
      Result result = null;
      byte[] value = null;
      
      int count = 0;
      
      Scan scan = new Scan(Bytes.toBytes("a"));
      scan.addFamily(Bytes.toBytes("p"));
      ResultScanner scanner = table.getScanner(scan);
      Map<?,?> familyMap = null;
      
      byte[] txtValue = null;
      byte[] nlpBinValue = null;
      
      FileOutputStream fos = null;
      
      String rowKey = null;
      
      while ((result = scanner.next()) != null && count <= 50000) {
        txtValue = result.getValue(Bytes.toBytes("p"),
            Bytes.toBytes("contact.txt"));
        nlpBinValue = result.getValue(Bytes.toBytes("p"),
            Bytes.toBytes("contact_nlp.bin"));
        if (txtValue == null || nlpBinValue == null) {
          continue;
        }
        rowKey = Bytes.toString(result.getRow());
        String[] strArray = rowKey.split("@");
        fos = new FileOutputStream(new File(outputDir.getAbsolutePath() + "/"
            + strArray[0] + "@contact.txt"));
        fos.write(txtValue);
        fos.close();
        fos = new FileOutputStream(new File(outputDir.getAbsolutePath() + "/"
            + strArray[0] + "@contact_nlp.bin"));
        fos.write(nlpBinValue);
        fos.close();
        count++;
      }
    } catch (Exception e) {
      LOG.warn("", e);
    }
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    baseDir = System.getProperty("base.dir");
    
    outputDir = new File(baseDir + "/nlp_export");
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    
    PropertyConfigurator.configure(baseDir + "/conf/log4j.properties");
    
    Configuration conf = new Configuration();
    // bj
    conf.set(
        "hbase.zookeeper.quorum",
        "mirage-pro.hbase0001.bj.voicecloud.cn,mirage-pro.hbase0002.bj.voicecloud.cn,mirage-pro.hbase0003.bj.voicecloud.cn");
    // hf
    // conf.set("hbase.zookeeper.quorum",
    // "mirage-pro.hbase0001.hf.voicecloud.cn");
    
    ExportNlp en = new ExportNlp();
    en.runTool(conf, args);
  }
  
}
