/*
 * All content copyright (c) 2003-2008 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tctest.restart.system;

import com.tc.test.MultipleServersCrashMode;
import com.tc.test.MultipleServersPersistenceMode;
import com.tc.test.MultipleServersSharedDataMode;
import com.tc.test.activepassive.ActivePassiveTestSetupManager;
import com.tc.test.restart.RestartTestHelper;
import com.tc.util.runtime.Os;
import com.tctest.Memory;
import com.tctest.TestConfigurator;
import com.tctest.TransparentTestBase;
import com.tctest.TransparentTestIface;
import com.tctest.modes.ActivePassiveTestMode;
import com.tctest.modes.CrashTestMode;
import com.tctest.modes.NormalTestMode;
import com.tctest.modes.NormalTestSetupManager;
import com.tctest.modes.TestMode;

public class ObjectDataTest extends TransparentTestBase implements TestConfigurator {

  private final int clientCount = 2;

  @Override
  protected Class<ObjectDataTestApp> getApplicationClass() {
    return ObjectDataTestApp.class;
  }

  @Override
  public void doSetUp(TransparentTestIface t) throws Exception {
    t.getTransparentAppConfig().setClientCount(clientCount).setIntensity(1);
    t.initializeTestRunner();
  }

  @Override
  protected long getRestartInterval(RestartTestHelper helper) {
    if (Os.isSolaris() || Memory.isMemoryLow()) {
      return super.getRestartInterval(helper) * 3;
    } else {
      return super.getRestartInterval(helper);
    }
  }

  @Override
  public TestMode[] getTestModes() {
    TestMode[] modes = new TestMode[4];

    modes[0] = new NormalTestMode();
    ((NormalTestSetupManager) modes[0].getSetupManager()).setPersistent(true);

    modes[1] = new NormalTestMode();
    ((NormalTestSetupManager) modes[1].getSetupManager()).setPersistent(false);

    modes[2] = new CrashTestMode();

    modes[3] = new ActivePassiveTestMode();
    setupActivePassiveTest((ActivePassiveTestSetupManager) modes[3].getSetupManager());

    return modes;
  }

  protected boolean canRunCrash() {
    return true;
  }

  public void setupActivePassiveTest(ActivePassiveTestSetupManager setupManager) {
    setupManager.setServerCount(2);
    setupManager.setServerCrashMode(MultipleServersCrashMode.CONTINUOUS_ACTIVE_CRASH);
    setupManager.setServerCrashWaitTimeInSec(30);
    setupManager.setServerShareDataMode(MultipleServersSharedDataMode.NETWORK);
    setupManager.setServerPersistenceMode(MultipleServersPersistenceMode.TEMPORARY_SWAP_ONLY);
  }
}
