package com.iflytek.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

class CellGet {
  public static final Log LOG = LogFactory.getLog(CellGet.class);
  public static final String ZK_QUORUM = "mirage-pro.hbase0001.gz.voicecloud.cn"; 
  
  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	conf.set("hbase.zookeeper.quorum", ZK_QUORUM);
    HTable table = new HTable(conf, "personal_");
    //Get get = new Get(Bytes.toBytes("201000000"));
    //get.addColumn(Bytes.toBytes("p"), Bytes.toBytes("contact.bin"));
    Scan scan = new Scan();
    //Result result = table.get(get);
    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    
    table.close();
  }
}
