package com.iflytek.personal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.iflytek.hbase.util.HbaseCell;

public class PersonalTest {
  
  private void printCell(HbaseCell cell) {
    System.out.println(cell.getRowKey());
    System.out.println(cell.getFamily());
    System.out.println(cell.getQualify());
    System.out.println(cell.getValue().length);
  }
  
  @Before
  public void setup() throws Exception {
    
  }
  
  @After
  public void cleanup() throws Exception {
    
  }
  
  @Test
  public void contactFileCase() throws Exception {
    String oldRowKey = "aa100000121";
    String oldFamily = "cf";
    String oldQualify = "/contact/a100000121@contact.bin";
    byte[] value = new byte[1];
    
    PersonalCell personal = new PersonalCell();
    personal.parsePersonalData(oldRowKey, oldFamily, oldQualify, value);
    printCell(personal.getHbaseCell());
  }
  
  @Test
  public void irfFileCase() throws Exception {
    
  }
  
  @Test
  public void wavFileCase() throws Exception {
    
  }
  
}
