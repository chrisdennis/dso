/*
 * All content copyright (c) 2003-2006 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.objectserver.handler;

import com.tc.async.api.AbstractEventHandler;
import com.tc.async.api.ConfigurationContext;
import com.tc.async.api.EventContext;
import com.tc.objectserver.tx.ServerTransactionManager;

public class ReceiveGroupMessageHandler extends AbstractEventHandler {
  private ServerTransactionManager transactionManager;

  public void handleEvent(EventContext context) {
    /*
    AcknowledgeTransactionMessage atm = (AcknowledgeTransactionMessage) context;
    transactionManager.acknowledgement(atm.getRequesterID(), atm.getRequestID(), atm.getClientID());
    */
  }

  public void initialize(ConfigurationContext context) {
    /*
    super.initialize(context);
    ServerConfigurationContext scc = (ServerConfigurationContext) context;
    this.transactionManager = scc.getTransactionManager();
    */
  }

}