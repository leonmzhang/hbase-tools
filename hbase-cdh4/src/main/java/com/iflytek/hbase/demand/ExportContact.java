package com.iflytek.hbase.demand;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ExportContact {
  
  public static Configuration proConf = new Configuration();
  public static Configuration testConf = new Configuration();
  
  public static HTable proTable;
  public static HTable testTable;
  
  public static int totalCount = 0;
  
  public static PrintWriter pw;
  
  static {
    proConf.set("hbase.zookeeper.quorum",
        "mirage-pro.hbase0001.hf.voicecloud.cn");
    testConf.set("hbase.zookeeper.quorum",
        "mirage-test.hbase0001.hf.voicecloud.cn");
    
    try {
      proTable = new HTable(proConf, "personal");
      testTable = new HTable(testConf, "personal");
      
      pw = new PrintWriter(new File("uid_list"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public static void copyData(String startKey) {
    int count = 0;
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("p"));
    String rowKey = null;
    try {
      ResultScanner scanner = proTable.getScanner(scan);
      Result result = null;
      while ((result = scanner.next()) != null && count < 10000) {
        rowKey = Bytes.toString(result.getRow());
        byte[] value = result.getValue(Bytes.toBytes("p"),
            Bytes.toBytes("contact.txt"));
        if (value != null) {
          Put put = new Put(Bytes.toBytes(rowKey));
          put.add(Bytes.toBytes("p"), Bytes.toBytes("contact.txt"), value);
          testTable.put(put);
          pw.write(rowKey + "\n");
          pw.flush();
          count++;
          System.out.println("already copy data: " + (++totalCount));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    String startKey = "";
    
    for (int i = 0; i < 10; i++) {
      if (i == 0) {
        startKey = "10";
      } else {
        startKey = "a" + i;
      }
      copyData(startKey);
    }
  }
}
