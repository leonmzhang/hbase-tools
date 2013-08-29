package com.iflytek.hbase.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.iflytek.hbase.thrift.generated.Hbase;
import com.iflytek.hbase.thrift.generated.Mutation;

public class ImportTxt {
  
  private static final String UID_ARRAY[] = {"1000001", "1000112", "50008",
      "a100000295", "a100000646", "a300000054", "a400000330", "a600000099",
      "a700000042", "a700000045"};
  
  private static final String zkQuorum = "192.168.52.13,192.168.52.14,"
      + "192.168.52.15,192.168.52.16,192.168.52.17";
  private static final String THRIFT_SERVER = "192.168.52.232";
  private static final String port = "9090";
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    
    TSocket socket = new TSocket(THRIFT_SERVER, 9090);
    socket.setTimeout(3000);
    TTransport transport = new TFramedTransport(socket);
    TProtocol protocol = new TBinaryProtocol(transport);
    try {
      transport.open();
    } catch (TTransportException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return;
    }
    
    Hbase.Client client = new Hbase.Client(protocol);
    
    List<Mutation> mutations = null;
    Mutation mutation = null;
    Map<ByteBuffer,ByteBuffer> attributes = new HashMap<ByteBuffer,ByteBuffer>();
    
    try {
      HTable table = new HTable(conf, "personal");
      Get get = null;
      Result result = null;
      Map<byte[],byte[]> familyMap = null;
      
      for (String uid : UID_ARRAY) {
        String oldUid = uid;
        if (uid.startsWith("a")) {
          oldUid = "a" + uid;
        }
        
        get = new Get(Bytes.toBytes(oldUid));
        get.addFamily(Bytes.toBytes("cf"));
        
        result = table.get(get);
        System.out.println("get row:" + Bytes.toString(result.getRow()));
        
        familyMap = result.getFamilyMap(Bytes.toBytes("cf"));
        
        for (Map.Entry<?,?> entry : familyMap.entrySet()) {
          String q = Bytes.toString((byte[]) entry.getKey());
          if (q.endsWith("txt")) {
            System.out.println("get q:" + q);
            byte[] v = (byte[]) entry.getKey();
            mutations = new ArrayList<Mutation>();
            mutation = new Mutation();
            mutation.setColumn(Bytes.toBytes("p:contact.txt"));
            mutation.setValue(v);
            mutations.add(mutation);
            ByteBuffer tableName = ByteBuffer.wrap(Bytes.toBytes("personal_"));
            ByteBuffer rowKey = ByteBuffer.wrap(Bytes.toBytes(uid));
            client.mutateRow(tableName, rowKey, mutations, attributes);
          }
        }
      }
      
      table.close();
      transport.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    
  }
}
