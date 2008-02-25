/*
 * All content copyright (c) 2003-2008 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.statistics.beans;

import com.tc.management.TerracottaMBean;

public interface StatisticsLocalGathererMBean extends TerracottaMBean {
  public void connect();

  public void disconnect();

  public void createSession(String sessionId);

  public void closeSession();

  public String[] getSupportedStatistics();

  public void enableStatistics(String[] names);

  public void startCapturing();

  public void stopCapturing();

  public void setGlobalParam(String key, Object value);

  public Object getGlobalParam(String key);

  public void setSessionParam(String key, Object value);

  public Object getSessionParam(String key);

  public void clearStatistics(String sessionId);

  public void clearAllStatistics();
}