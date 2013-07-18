package com.iflytek.personal;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.iflytek.hbase.Common;
import com.iflytek.hbase.Constants;

public class RowMessage {
  private StringBuilder msg = new StringBuilder();
  private boolean needAppendRow = true;
  
  public RowMessage() {
    
  }
  
  public StringBuilder getMsg() {
    return msg;
  }
  
  public void appendCellMsg(Personal personal) {
    
  }
  
  public void appendRow(String row) {
    if (needAppendRow) {
      msg.append(row + Constants.LINE_SEPARATOR);
      needAppendRow = false;
    }    
  }
  
  public void appendCellMsg(String family, String qualify,
      long timestamp, int valueLength, BigInteger digest) {
    String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(
        timestamp));
    String space4 = "    ";
    msg.append(space4);
    msg.append(Common.completionString(family + ":" + qualify, ' ', 64, false));
    msg.append(space4);
    msg.append(Common.completionString("" + valueLength, 10));
    msg.append(space4);
    msg.append(date);
    msg.append(space4);
    msg.append(Common.completionString(digest.toString(16), '0', 32, true));
    msg.append(Constants.LINE_SEPARATOR);
  }
}
