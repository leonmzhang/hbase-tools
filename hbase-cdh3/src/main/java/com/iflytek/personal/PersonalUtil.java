package com.iflytek.personal;

import java.util.HashMap;
import java.util.Map;

public class PersonalUtil {
  public static final String PERSONAL = "personal";
  public static final String PERSONAL_IRF = "personal_irf";
  public static final String PERSONAL_WAV = "personal_wav";
  
//  public static final String PERSONAL = "personal_";
//  public static final String PERSONAL_IRF = "personal_irf_";
//  public static final String PERSONAL_WAV = "personal_wav_";
  
  public static final String[] KEY = {"00;a00", "aa00;aa05", "aa05;aa10",
      "aa10;aa15", "aa15;aa20", "aa20;aa25", "aa25;aa30", "aa30;aa35",
      "aa35;aa40", "aa40;aa45", "aa45;aa50", "aa50;aa55", "aa55;aa60",
      "aa60;aa65", "aa65;aa70", "aa70;aa75", "aa75;aa80", "aa80;aa85",
      "aa85;aa90", "aa90;aa95", "aa95;aa99"};
  
  // 个性化文件类型
  public static final String CONTACT_BIN = "contact.bin";
  public static final String CONTACT_TXT = "contact.txt";
  public static final String CONTACT_NLP_BIN = "contact_nlp.bin";
  public static final String CONTACT_NLP_TXT = "contact_nlp.txt";
  public static final String CONTACT_NLP_LESS = "contact_nlp.less";
  public static final String USERWORD_TXT = "userword.txt";
  public static final String USERWORD_WORD_TXT = "userword_word.txt";
  public static final String USERWORD_WORD_BIN = "userword_word.bin";
  
  // 路径前缀
  public static final String CONTACT = "contact";
  public static final String USER_WORD = "userword";
  public static final String TTS = "tts";
  public static final String PANDA = "panda";
  public static final String TVWORD = "tvword";
  
  public static Map<String,String> typeMap;
  public static Map<String,String> prefixMap;
  
  static {
    typeMap = new HashMap<String,String>();
    typeMap.put(CONTACT_BIN, "");
    typeMap.put(CONTACT_TXT, "");
    typeMap.put(CONTACT_NLP_BIN, "");
    typeMap.put(CONTACT_NLP_TXT, "");
    typeMap.put(CONTACT_NLP_LESS, "");
    typeMap.put(USERWORD_TXT, "");
    typeMap.put(USERWORD_WORD_TXT, "");
    typeMap.put(USERWORD_WORD_BIN, "");
    
    prefixMap = new HashMap<String,String>();
    prefixMap.put(CONTACT, "");
    prefixMap.put(USER_WORD, "");
    prefixMap.put(TTS, "");
    prefixMap.put(PANDA, "");
    prefixMap.put(TVWORD, "");
  }
  
  public static StringBuilder buildRowMsg(String row) {
    StringBuilder sb = new StringBuilder();
    
    return sb;
  }
}
