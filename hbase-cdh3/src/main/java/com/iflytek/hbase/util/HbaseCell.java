package com.iflytek.hbase.util;

import java.util.Arrays;

public class HbaseCell {
  private String table;
  private String rowKey;
  private String family;
  private String qualify;
  private String column;
  private long timestamp;
  private byte[] value;
  
  public HbaseCell() {
    
  }
  
  public HbaseCell(HbaseCell cell) {
    setRowKey(cell.getRowKey());
    setFamily(cell.getFamily());
    setQualify(cell.getQualify());
    setValue(cell.getValue());
  }
  
  public String getTable() {
    return this.table;
  }
  
  public void setTable(String table) {
    this.table = table;
  }
  
  public String getRowKey() {
    return rowKey;
  }
  
  public void setRowKey(String rowKey) {
    this.rowKey = rowKey;
  }
  
  public String getFamily() {
    return family;
  }
  
  public void setFamily(String family) {
    this.family = family;
  }
  
  public String getQualify() {
    return qualify;
  }
  
  public void setQualify(String qualify) {
    this.qualify = qualify;
  }
  
  public String getColumn() {
    return family + ":" + qualify;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public byte[] getValue() {
    return value;
  }
  
  public void setValue(byte[] value) {
    this.value = Arrays.copyOf(value, value.length);
  }
  
  @Override
  public String toString() {
    return null;
  }
  
  @Override
  public HbaseCell clone() {
    return new HbaseCell(this);
  }
}
