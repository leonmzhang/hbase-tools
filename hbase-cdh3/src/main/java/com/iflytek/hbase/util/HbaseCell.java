package com.iflytek.hbase.util;

import java.util.Arrays;

public class HbaseCell {
  private String rowKey;
  private String family;
  private String qualify;
  private byte[] value;
  
  public HbaseCell() {
    
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
  
  public byte[] getValue() {
    return value;
  }
  
  public void setValue(byte[] value) {
    this.value = Arrays.copyOf(value, value.length);
  }
  
  public HbaseCell(HbaseCell cell) {
    setRowKey(cell.getRowKey());
    setFamily(cell.getFamily());
    setQualify(cell.getQualify());
    setValue(cell.getValue());
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
