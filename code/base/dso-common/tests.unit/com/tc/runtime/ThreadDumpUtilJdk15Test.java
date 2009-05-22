/*
 * All content copyright (c) 2003-2008 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.runtime;

import com.tc.util.Assert;
import com.tc.util.runtime.ThreadDumpUtilTestBase;

public class ThreadDumpUtilJdk15Test extends ThreadDumpUtilTestBase {

  public void testThreadDump15() throws Exception {
    final int numThreads = 10;
    String dump = getDump(numThreads, TraceThread.class);
    
    Assert.eval("The text \"Full thread dump \" should be present in the thread dump",
                dump.indexOf("Full thread dump ") >= 0);
    
    // we expect to see all the created threads waiting on a CountDownLatch
    assertEquals(numThreads, countSubstrings(dump, CDL_AWAIT));
  }

  /**
   * Thread.getId() should be final but it isn't, so subclasses can break the contract.
   * When this happens we need to behave gracefully.  See CDV-1262.
   */
  public void testBadThreadId() throws Exception {
    final int numThreads = 10;
    String dump = getDump(numThreads, BadIdThread.class);
    
    Assert.eval("The text \"Full thread dump \" should be present in the thread dump",
                dump.indexOf("Full thread dump ") >= 0);
    
    // we expect to see all the created threads waiting on a CountDownLatch
    assertEquals(numThreads, countSubstrings(dump, CDL_AWAIT));
    
    // half the strings should be complaining about unrecognized IDs
    assertEquals(numThreads / 2, countSubstrings(dump, OVERRIDDEN));
  }
  
}

