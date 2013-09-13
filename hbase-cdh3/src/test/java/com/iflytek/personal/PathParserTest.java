package com.iflytek.personal;

import java.util.ArrayList;

import org.junit.Test;

public class PathParserTest {
  public static void print(String str) {
    System.out.println(str);
  }
  
  @Test
  public void fullPathParseCase() throws Exception {
    ArrayList<String> pathList = new ArrayList<String>();
    pathList
        .add("/msp/user/anonymous/aa6/g35/p41/7804/contact/a635417804@contact.txt");
    pathList
        .add("/msp/user/anonymous/aa5/g39/p15/9211/contact/a539159211@contact.bin");
    pathList
        .add("/msp/user/anonymous/aa6/g35/p41/7804/contact/a635417804@contact_nlp.bin");
    pathList
        .add("/msp/user/anonymous/aa6/g35/p41/7804/contact/a635417804@contact_nlp.less");
    pathList
        .add("/msp/user/anonymous/aa5/g60/p34/5367/4edca818/contact/a560345367@contact.txt");
    pathList
        .add("/msp/user/anonymous/aa5/g60/p34/5367/4edca818/contact/a560345367@contact.bin");
    pathList
        .add("/msp/user/anonymous/aa5/g60/p34/5367/4edca818/contact/a560345367@contact_nlp.bin");
    pathList
        .add("/msp/user/anonymous/aa5/g60/p34/5367/4edca818/contact/a560345367@contact_nlp.less");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/contact/1459275@contact.txt");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/contact/1459275@contact.bin");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/contact/1459275@contact_nlp.bin");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/contact/1459275@contact_nlp.less");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/4edca818/contact/1459275@contact.txt");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/4edca818/contact/1459275@contact.bin");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/4edca818/contact/1459275@contact_nlp.bin");
    pathList
        .add("/msp/user/register/a00/g01/p45/9275/4edca818/contact/1459275@contact_nlp.less");
    pathList.add("/msp/app/4f58bb41/app_4f58bb41@reslist.bin");
    pathList
        .add("/msp/user/anonymous/a00/g00/p0a/1000/vp/06f03b50ef2a5ed9de28d8b91c56c5314@vp.irf");
    pathList
        .add("/msp/user/register/a00/g00/p00/0001/tts/gszhang/wav/00000001.wav");
    pathList.add("/msp/user/register/a00/g00/p00/0043/hotkey/hotkey.txt");
    pathList.add("//////////////////////");
    pathList.add("\\\\\\\\\\\\\\\\\\\\\\\\");
    pathList.add("@@@@@@@@@@@@@@@@@@@@@@@@");
    pathList.add("........................");
    pathList.add("/msp/user/register/a00/");
    pathList.add("");
    pathList.add("/msp_gws/2012-11-29_panguso_hotwords.txt");
    pathList.add("/msp/app/bbk/app_BBK@reslist.txt");
    pathList
        .add("/msp/user/anonymous/a0a/g14/p46/1768/panda/a14461768@panda_knowledge");
    pathList.add("/msp/user/register/a00/g00/p09/0000/xiali/90000@xiali.txt");
    pathList
        .add("/msp/user/anonymous/a0a/g14/p46/1768/panda/a14461768@panda_knowledge.bin");
    
    for (String path : pathList) {
      try {
        print("==========================================================================================");
        print("process path: " + path);
        PathParser pp = new PathParser();
        pp.parseFullPath(path);
        print("old table: " + pp.oldTable);
        print("old row key: " + pp.oldRowKey);
        print("old family: " + pp.oldFamily);
        print("old qualify:" + pp.oldQualify);
        print("old column:" + pp.oldColumn);
        print("------------------------------------------------------------------------------------------");
        print("new table: " + pp.newTable);
        print("new row key: " + pp.newRowKey);
        print("new family: " + pp.newFamily);
        print("new qualify:" + pp.newQualify);
        print("new column:" + pp.newColumn);
        print("==========================================================================================");
      } catch (Exception e) {
        e.printStackTrace(System.out);
        print("==========================================================================================");
        continue;
      }
    }
  }
  
  @Test
  public void partPathParseCase() throws Exception {
    ArrayList<String> pathList = new ArrayList<String>();
    pathList.add("/contact/a635417804@contact.txt");
    pathList.add("/contact/a539159211@contact.bin");
    pathList.add("/contact/a635417804@contact_nlp.bin");
    pathList.add("/contact/a635417804@contact_nlp.less");
    pathList.add("/4edca818/contact/a560345367@contact.txt");
    pathList.add("/4edca818/contact/a560345367@contact.bin");
    pathList.add("/4edca818/contact/a560345367@contact_nlp.bin");
    pathList.add("/4edca818/contact/a560345367@contact_nlp.less");
    pathList.add("/contact/1459275@contact.txt");
    pathList.add("/contact/1459275@contact.bin");
    pathList.add("/contact/1459275@contact_nlp.bin");
    pathList.add("/contact/1459275@contact_nlp.less");
    pathList.add("/4edca818/contact/1459275@contact.txt");
    pathList.add("/4edca818/contact/1459275@contact.bin");
    pathList.add("/4edca818/contact/1459275@contact_nlp.bin");
    pathList.add("/4edca818/contact/1459275@contact_nlp.less");
    pathList
        .add("/msp/user/anonymous/a00/g00/p0a/1000/vp/06f03b50ef2a5ed9de28d8b91c56c5314@vp.irf");
    pathList
        .add("/msp/user/register/a00/g00/p00/0001/tts/gszhang/wav/00000001.wav");
    pathList.add("/hotkey/hotkey.txt");
    pathList.add("//////////////////////");
    pathList.add("\\\\\\\\\\\\\\\\\\\\\\\\");
    pathList.add("@@@@@@@@@@@@@@@@@@@@@@@@");
    pathList.add("........................");
    pathList.add("/msp/user/register/a00/");
    pathList.add("");
    pathList.add("/panda/a14461768@panda_knowledge");
    pathList.add("/xiali/90000@xiali.txt");
    pathList.add("/panda/a14461768@panda_knowledge.bin");
    
    for (String path : pathList) {
      try {
        print("==========================================================================================");
        print("process path: " + path);
        PathParser pp = new PathParser();
        pp.parsePartPath("test", path);
        print("old table: " + pp.oldTable);
        print("old row key: " + pp.oldRowKey);
        print("old family: " + pp.oldFamily);
        print("old qualify:" + pp.oldQualify);
        print("old column:" + pp.oldColumn);
        print("------------------------------------------------------------------------------------------");
        print("new table: " + pp.newTable);
        print("new row key: " + pp.newRowKey);
        print("new family: " + pp.newFamily);
        print("new qualify:" + pp.newQualify);
        print("new column:" + pp.newColumn);
        print("==========================================================================================");
      } catch (Exception e) {
        e.printStackTrace(System.out);
        print("==========================================================================================");
        continue;
      }
    }
  }
  
}
