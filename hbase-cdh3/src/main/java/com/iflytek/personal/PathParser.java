package com.iflytek.personal;

public class PathParser {
  public String table = "personal";
  public String rowKey = "";
  public String family = "p";
  public String qualify = "";
  public String column = "";
  
  public void parseFullPath(String oldPath) throws PersonalParseException {

    return;
  }
  
  public void parsePartPath(String rowKey, String oldPath) throws PersonalParseException {
    this.rowKey = rowKey;
    String[] pathArray = oldPath.split("@");
    if(pathArray.length != 2) {
      return;
    } else {
      qualify = pathArray[1];
      column = family + ":" + qualify;
    }
    return;
  }
  
  public static void main(String[] args) {
    
  }
}
