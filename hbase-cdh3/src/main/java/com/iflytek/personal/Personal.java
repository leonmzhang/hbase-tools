package com.iflytek.personal;

import java.util.HashMap;
import java.util.Map;

import com.iflytek.hbase.util.HbaseCell;

public class Personal {
  private static final String APPID = "appid";
  private static final String IRF = "irf";
  private static final String WAV = "wav";
  private static final String ROOT = "root";
  private static final String FILE_NAME = "file_name";
  private static final String TYPE = "type";
  private static final String TABLE = "table";
  
  private HbaseCell cell = new HbaseCell();
  
  public Personal() {
    
  }
  
  public Personal(Personal p) {
    
  }
  
  public HbaseCell getHbaseCell() {
    return cell;
  }
  
  public void parsePersonalData(String oldRowKey, String oldFamily,
      String oldQualify, byte[] value) throws PersonalParseException {
    if (oldRowKey == null) {
      throw new PersonalParseException();
    }
    
    String rowKey =cleanRowKey(oldRowKey); 

    cell.setFamily("p");
    
    Map<String,String> kvMap = parseQualify(oldQualify);
    String appid = null;
    if((appid = kvMap.get(APPID)) != null) {
      rowKey = rowKey + "@" + appid;
    }
    cell.setTable(kvMap.get(TABLE));
    cell.setRowKey(rowKey);
    cell.setQualify(kvMap.get(TYPE));
    cell.setValue(value);
  }
  
  private String cleanRowKey(String oldRowKey) {
    if (oldRowKey != null && oldRowKey.startsWith("aa")) {
      return oldRowKey.substring(1);
    } else {
      return oldRowKey;
    }
  }
  
  private Map<String,String> parseQualify(String qualify)
      throws PersonalParseException {
    Map<String,String> kvMap = new HashMap<String,String>();
    
    if (qualify == null) {
      throw new PersonalParseException("qualify: null");
    }
    
    String qualifyEx = qualify;
    while (qualifyEx.startsWith("/")) {
      qualifyEx = qualifyEx.substring(1);
    }
    String[] pathArray = qualifyEx.split("/");
    
    // check file type
    String fileName = pathArray[pathArray.length - 1];
    kvMap.put(FILE_NAME, fileName);
    
    String[] fileNameArray = fileName.split("@");
    String type = null;
    String prefix = null;
    String table = null;
    if (fileName.contains(IRF)) {
      type = IRF;
      table = PersonalUtil.PERSONAL_IRF;
    } else if (fileName.contains(WAV)) {
      type = WAV;
      table = PersonalUtil.PERSONAL_WAV;
    } else {
      table = PersonalUtil.PERSONAL;
      prefix = pathArray[0];
      if (ROOT.equals(prefix)) {
        throw new PersonalParseException("qualify: " + qualify);
      }
      type = fileNameArray[fileNameArray.length - 1];
      
      // check appid
      if (PersonalUtil.prefixMap.get(prefix) == null) {
        String appid = prefix;
        kvMap.put(APPID, appid);
      }
    }
    kvMap.put(TYPE, type);
    kvMap.put(TABLE, table);
    return kvMap;
  }
}
