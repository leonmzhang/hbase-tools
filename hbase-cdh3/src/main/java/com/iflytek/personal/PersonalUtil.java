package com.iflytek.personal;

public class PersonalUtil {
  
  public static final String cleanRowKey(String oldRowKey) {
    if (oldRowKey.startsWith("aa")) {
      return oldRowKey.substring(1);
    } else {
      return oldRowKey;
    }
  }
}
