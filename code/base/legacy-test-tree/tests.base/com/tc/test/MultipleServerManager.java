/*
 * All content copyright (c) 2003-2008 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.test;

import com.tc.exception.ImplementMe;
import com.tc.test.proxyconnect.ProxyConnectManager;

import java.util.List;

public abstract class MultipleServerManager {

  public static final String CONFIG_FILE_NAME = "multiple-servers-config.xml";
  
  protected final MultipleServersTestSetupManager setupManger;
  
  public MultipleServerManager(MultipleServersTestSetupManager setupManger) {
    this.setupManger = setupManger;
  }

  public abstract ProxyConnectManager[] getL2ProxyManagers();

  public abstract List getErrors();

  public abstract void stopAllServers() throws Exception;

  public abstract void dumpAllServers(int currentPid, int dumpCount, long dumpInterval) throws Exception;

  public MultipleServersTestSetupManager getMultipleServersTestManager() {
    return setupManger;
  }

  public abstract void crashActiveServers() throws Exception;

  public int getPid() {
    throw new ImplementMe();
  }

}