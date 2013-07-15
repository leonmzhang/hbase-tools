package com.iflytek.hbase;

import org.junit.Test;

public class ScanToolTest {
  
  @Test
  public void usageCase() throws Exception {
    ScanTool st = new ScanTool();
    st.usage();
  }
}
