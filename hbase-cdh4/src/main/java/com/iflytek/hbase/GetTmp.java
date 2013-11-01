package com.iflytek.hbase;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetTmp {
  public static void print(String msg) {
    System.out.println(msg);
  } 
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum",
        "192.168.151.101,192.168.151.102,192.168.151.103");
    HTable table = new HTable(conf, "personal");
    
    String[] uids = {"a631317411", "a287585594", "a288655763", "a345402809"};
    for(String uid : uids) {
      Get get = new Get(Bytes.toBytes(uid));
      Result result = table.get(get);
      byte[] value = result.getValue(Bytes.toBytes("p"), Bytes.toBytes("contact.txt"));
      FileOutputStream fos = new FileOutputStream(new File(uid + "@contact.txt"));
      fos.write(value);
      fos.close();  
    }
    
    table.close();
  }
}
