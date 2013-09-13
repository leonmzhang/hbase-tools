package com.iflytek.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.iflytek.hbase.thrift.generated.Hbase;
import com.iflytek.hbase.thrift.generated.Mutation;
import com.iflytek.hbase.thrift.generated.TCell;
import com.iflytek.personal.PathParser;
import com.iflytek.personal.PersonalCell;
import com.iflytek.personal.PersonalParseException;
import com.iflytek.personal.PersonalUtil;

public class SyncList {
  private static final Log LOG = LogFactory.getLog(SyncList.class);
  
  private static Options options = new Options();
  private static CommandLine cmdLine;
  
  private String thriftServers = Constants.GZ_HBASE_THRIFT_SERVERS;
  
  static {
    options.addOption("l", "list", true, "");
    
  }
  
  private String getDesThriftServer() {
    String[] serversArray = thriftServers.split(",");
    int index = (int) (Math.random() * (float) serversArray.length);
    return serversArray[index];
  }
  
  public int runTool(Configuration conf, String[] args) throws Exception {
    cmdLine = Common.parseOptions(options, args);
    if (!cmdLine.hasOption('l')) {
      System.out.println("no list file specify");
      return -1;
    }
    
    String listFilePath = cmdLine.getOptionValue('l');
    File file = new File(listFilePath);
    FileReader fr = new FileReader(file);
    BufferedReader br = new BufferedReader(fr);
    String line = null;
    
    conf.set(Constants.HBASE_ZOOKEEPER_QUORUM,
        Constants.GZ_HBASE_ZOOKEEPER_QUORUM);
    HTable table = new HTable(conf, "personal");
    Result result = null;
    Map<byte[],byte[]> familyMap = null;
    
    String oldRowKey = null;
    String oldQualify = null;
    long oldTimestamp = 0;
    byte[] value = null;
    
    String newTable = null;
    String newRowKey = null;
    String newQualify = null;
    String newColumn = null;
    ByteBuffer newTableByte = null;
    ByteBuffer newRowByte = null;
    ByteBuffer newColumnByte = null;
    
    TTransport transport;
    TProtocol protocol;
    Hbase.Client client;
    
    String host = getDesThriftServer();
    int port = 9090;
    transport = new TSocket(host, port);
    protocol = new TBinaryProtocol(transport);
    client = new Hbase.Client(protocol);
    transport.open();
    
    List<Mutation> mutations = new ArrayList<Mutation>();
    Mutation mutation = null;
    Map<ByteBuffer,ByteBuffer> attributes = new HashMap<ByteBuffer,ByteBuffer>();
    
    while ((line = br.readLine()) != null) {
      LOG.info("proecess: " + line);
      PathParser pp = new PathParser();
      try {
        pp.parseFullPath(line);
      } catch (Exception e) {
        LOG.warn("parse path failed, path: " + line);
        continue;
      }
      oldRowKey = pp.oldRowKey;
      Get get = new Get(Bytes.toBytes(oldRowKey));
      result = table.get(get);
      
      familyMap = result.getFamilyMap(PersonalUtil.OLD_FAMILY_BYTE);
      for (Map.Entry<?,?> entry : familyMap.entrySet()) {
        oldQualify = Bytes.toString((byte[]) entry.getKey());
        oldTimestamp = result.getColumnLatest(PersonalUtil.OLD_FAMILY_BYTE,
            Bytes.toBytes(oldQualify)).getTimestamp();
        value = (byte[]) entry.getValue();
        LOG.info("get old personal cell, row: " + oldRowKey + ", column: "
            + PersonalUtil.OLD_FAMILY_STR + ":" + oldQualify
            + ", modify time: " + Common.unixTimestampToDateStr(oldTimestamp)
            + ", old path: " + oldQualify);
        
        PersonalCell personal = new PersonalCell();
        try {
          // personal.parsePersonalData(oldRowKey, PersonalUtil.OLD_FAMILY_STR,
          // oldQualify, value);
          // newTable = personal.getHbaseCell().getTable();
          // newRowKey = personal.getHbaseCell().getRowKey();
          // newColumn = personal.getHbaseCell().getColumn();
          newTable = pp.newTable;
          newRowKey = pp.newRowKey;
          newColumn = pp.newColumn;
          
          mutations.clear();
          mutation = new Mutation();
          mutation.setColumn(Bytes.toBytes(newColumn));
          mutation.setValue(value);
          mutations.add(mutation);
          
          newTableByte = ByteBuffer.wrap(Bytes.toBytes(newTable));
          newRowByte = ByteBuffer.wrap(Bytes.toBytes(newRowKey));
          newColumnByte = ByteBuffer.wrap(Bytes.toBytes(newColumn));
          
          List<TCell> cellList = client.get(newTableByte, newRowByte,
              newColumnByte, attributes);
          if (cellList.isEmpty() || cellList.get(0).timestamp < oldTimestamp) {
            LOG.info("sync cell, table: " + newTable + ", row: " + newRowKey
                + ", column: " + newColumn + ", modify time: "
                + Common.unixTimestampToDateStr(oldTimestamp));
//            client.mutateRowTs(newTableByte, newRowByte, mutations,
//                oldTimestamp, attributes);
          } else {
            LOG.info("do not need to sync.");
          }
        } catch (Exception e) {
          LOG.warn("", e);
        }
      }
      
    }
    
    return 0;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    SyncList syncList = new SyncList();
    syncList.runTool(conf, args);
    
  }
  
}
