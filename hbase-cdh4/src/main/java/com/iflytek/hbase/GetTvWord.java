package com.iflytek.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

public class GetTvWord {
  private static final Log LOG = LogFactory.getLog(GetTvWord.class);
  private static String baseDir = "";
  
  public int runTool(Configuration conf, String[] args) {
    try {
      HTable table = new HTable(conf, "personal");
      
      FileReader fr = new FileReader(new File(baseDir + "/tv.txt"));
      BufferedReader br = new BufferedReader(fr);
      String line = null;
      Result result = null;
      byte[] value = null;
      String valueStr = null;
      while((line = br.readLine()) != null) {
        
        Get get = new Get(Bytes.toBytes(line));
        get.addColumn(Bytes.toBytes("p"), Bytes.toBytes("_v2_tvword_nlp.bin"));
        result = table.get(get);
        value = result.getValue(Bytes.toBytes("p"), Bytes.toBytes("_v2_tvword_nlp.bin"));
        valueStr = Bytes.toString(value);
        System.out.println("row: " + line + ", head: " + valueStr.substring(0, 3));
      }
     
      br.close();
      fr.close();
      table.close();
      
    } catch (IOException e) {
      LOG.warn("", e);
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
    PropertyConfigurator.configure(baseDir + "/conf/log4j.properties");
    
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", "mirage-pro.hbase0001.bj.voicecloud.cn,mirage-pro.hbase0002.bj.voicecloud.cn,mirage-pro.hbase0003.bj.voicecloud.cn");
    
    GetTvWord gtw = new GetTvWord();
    gtw.runTool(conf, args);
  }
  
}
