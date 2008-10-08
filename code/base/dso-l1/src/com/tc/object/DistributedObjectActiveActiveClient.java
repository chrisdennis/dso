/*
 * All content copyright (c) 2003-20067 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.object;

import com.tc.cluster.Cluster;
import com.tc.config.schema.dynamic.ConfigItem;
import com.tc.lang.TCThreadGroup;
import com.tc.net.core.ConnectionAddressProvider;
import com.tc.net.core.ConnectionInfo;
import com.tc.net.protocol.NetworkStackHarnessFactory;
import com.tc.net.protocol.clientgroup.ClientGroupCommunicationsManager;
import com.tc.net.protocol.clientgroup.ClientGroupCommunicationsManagerImpl;
import com.tc.net.protocol.tcm.ClientMessageChannel;
import com.tc.net.protocol.tcm.CommunicationsManager;
import com.tc.net.protocol.tcm.MessageMonitor;
import com.tc.net.protocol.transport.ConnectionPolicy;
import com.tc.net.protocol.transport.HealthCheckerConfig;
import com.tc.object.bytecode.Manager;
import com.tc.object.bytecode.hook.impl.PreparedComponentsFromL2Connection;
import com.tc.object.config.DSOClientConfigHelper;
import com.tc.object.loaders.ClassProvider;
import com.tc.object.session.SessionProvider;

/**
 * This is the main point of entry into the DSO active-active client.
 */
public class DistributedObjectActiveActiveClient extends DistributedObjectClient {
  private ClientGroupCommunicationsManager clientGroupCommunicationsManager = null;

  public DistributedObjectActiveActiveClient(DSOClientConfigHelper config, TCThreadGroup threadGroup,
                                             ClassProvider classProvider,
                                             PreparedComponentsFromL2Connection connectionComponents, Manager manager,
                                             Cluster cluster) {
    super(config, threadGroup, classProvider, connectionComponents, manager, cluster);

  }

  protected CommunicationsManager createCommunicationsManager(MessageMonitor monitor,
                                                              NetworkStackHarnessFactory stackHarnessFactory,
                                                              ConnectionPolicy connectionPolicy,
                                                              HealthCheckerConfig aConfig) {
    clientGroupCommunicationsManager = new ClientGroupCommunicationsManagerImpl(monitor, stackHarnessFactory,
                                                                                connectionPolicy, aConfig);
    return clientGroupCommunicationsManager;
  }

  protected ClientMessageChannel createChannel(CommunicationsManager commMgr,
                                               PreparedComponentsFromL2Connection connComp,
                                               SessionProvider sessionProvider) {
    ClientMessageChannel cmc;
    ConfigItem[] connectionInfoItems = connComp.createConnectionInfoConfigItemByGroup();
    ConnectionAddressProvider[] addrProviders = new ConnectionAddressProvider[connectionInfoItems.length];
    for (int i = 0; i < connectionInfoItems.length; ++i) {
      ConnectionInfo[] connectionInfo = (ConnectionInfo[]) connectionInfoItems[i].getObject();
      addrProviders[i] = new ConnectionAddressProvider(connectionInfo);
    }

    cmc = clientGroupCommunicationsManager.createClientGroupChannel(sessionProvider, -1, 10000, addrProviders);
    return (cmc);
  }

}
