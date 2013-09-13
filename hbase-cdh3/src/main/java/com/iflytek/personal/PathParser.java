package com.iflytek.personal;

import java.util.ArrayList;

public class PathParser {
  
  private static final String[] TYPE_ARRAY = {"contact", "userword", "tts",
      "panda", "tvword"};
  
  public String oldTable = "personal";
  public String oldRowKey = "";
  public String oldFamily = "cf";
  public String oldQualify = "";
  public String oldColumn = "";
  
  public String newTable = "personal";
  public String newRowKey = "";
  public String newFamily = "p";
  public String newQualify = "";
  public String newColumn = "";
  
  public void parseFullPath(String oldPath) throws PersonalParseException {
    if (oldPath == null || oldPath.length() == 0) {
      throw new PersonalParseException("");
    }
    String oldPathEx = oldPath.substring(1);
    // split path by
    String[] pathArray = oldPathEx.split("/");
    if (pathArray.length == 0) {
      throw new PersonalParseException("");
    }
    
    String fileName = pathArray[pathArray.length - 1];
    String[] fileNameArray = fileName.split("@");
    if (fileNameArray.length != 2) {
      newTable = "personal_other";
      newRowKey = oldPath;
      newQualify = "file";
      newColumn = "p:file";
      return;
    }
    
    String[] fileExtensionArray = fileNameArray[fileNameArray.length - 1]
        .split("\\.");
    if (fileExtensionArray.length == 0) {
      throw new PersonalParseException("");
    }
    
    String fileType = fileExtensionArray.length == 2 ? fileExtensionArray[1]
        : "";
    if (fileType.length() > 0 && fileType.equals("irf")) {
      newTable = "personal_irf";
      newRowKey = fileName;
      newQualify = "irf";
      newColumn = "p:irf";
      return;
    }
    
    if (pathArray.length < 3) {
      throw new PersonalParseException("");
    } else if (pathArray[1].equals("app")) {
      newTable = "personal";
      newRowKey = fileNameArray[0];
      newFamily = "p";
      newQualify = fileNameArray[1];
      newColumn = "p:" + fileNameArray[1];
      return;
    }
    
    String uid = fileNameArray[0];
    String appid = null;
    boolean appidFlag = true;
    if (pathArray.length < 9) {
      throw new PersonalParseException("");
    }
    
    if (pathArray[7].equals("root")) {
      throw new PersonalParseException("");
    }
    for (String type : TYPE_ARRAY) {
      if (type.equals(pathArray[7])) {
        appidFlag = false;
      }
    }
    if(appidFlag) {
      appid = pathArray[7];
    }
    
    oldRowKey = (uid.startsWith("a") ? "a" : "") + uid;
    
    newTable = "personal";
    newRowKey = uid + (appid == null ? "" : ("@" + appid));
    newFamily = "p";
    newQualify = fileNameArray[1];
    newColumn = "p:" + newQualify;
    return;
  }
  
  public void parsePartPath(String rowKey, String oldPath)
      throws PersonalParseException {
    this.newRowKey = rowKey;
    String[] pathArray = oldPath.split("@");
    if (pathArray.length != 2) {
      return;
    } else {
      newQualify = pathArray[1];
      newColumn = newFamily + ":" + newQualify;
    }
    return;
  }
}
