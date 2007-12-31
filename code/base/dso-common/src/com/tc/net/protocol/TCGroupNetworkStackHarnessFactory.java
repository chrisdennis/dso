/*
 * All content copyright (c) 2003-2006 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.net.protocol;

import com.tc.net.protocol.tcm.MessageChannelInternal;
import com.tc.net.protocol.tcm.ServerMessageChannelFactory;
import com.tc.net.protocol.transport.ClientConnectionEstablisher;
import com.tc.net.protocol.transport.ClientMessageTransport;
import com.tc.net.protocol.transport.ConnectionWatcher;
import com.tc.net.protocol.transport.MessageTransport;
import com.tc.net.protocol.transport.MessageTransportFactory;
import com.tc.net.protocol.transport.MessageTransportListener;

/**
 * TC-Group-Comm (no guarenteed messages) network stack harness factory
 */
public class TCGroupNetworkStackHarnessFactory implements NetworkStackHarnessFactory {

  private final boolean allowConnectionReplace;

  public TCGroupNetworkStackHarnessFactory() {
    this(false);
  }

  public TCGroupNetworkStackHarnessFactory(boolean allowConnectionReplace) {
    this.allowConnectionReplace = allowConnectionReplace;
  }

  public NetworkStackHarness createServerHarness(ServerMessageChannelFactory channelFactory,
                                                 MessageTransport transport,
                                                 MessageTransportListener[] transportListeners) {
    return new TCGroupNetworkStackHarness(channelFactory, transport);
  }

  public NetworkStackHarness createClientHarness(MessageTransportFactory transportFactory,
                                                 MessageChannelInternal channel,
                                                 MessageTransportListener[] transportListeners) {
    return new TCGroupNetworkStackHarness(transportFactory, channel);
  }

  private class TCGroupNetworkStackHarness extends AbstractNetworkStackHarness {

    TCGroupNetworkStackHarness(ServerMessageChannelFactory channelFactory, MessageTransport transport) {
      super(channelFactory, transport);
    }

    TCGroupNetworkStackHarness(MessageTransportFactory transportFactory, MessageChannelInternal channel) {
      super(transportFactory, channel);
    }

    protected void connectStack() {
      // for TC-Group-Comm, embed NodeID to ChannelID
      channel.getChannelID().setNodeID(transport.getConnectionId().getNodeID());
      
      channel.setSendLayer(transport);
      transport.setReceiveLayer(channel);
      transport.setAllowConnectionReplace(allowConnectionReplace);

      // XXX: this is super ugly, but...
      if (transport instanceof ClientMessageTransport) {
        ClientMessageTransport cmt = (ClientMessageTransport) transport;
        ClientConnectionEstablisher cce = cmt.getConnectionEstablisher();
        ConnectionWatcher cw = new ConnectionWatcher(cmt, channel, cce);
        transport.addTransportListener(cw);
      } else {
        transport.addTransportListener(channel);
      }
    }

    protected void createIntermediateLayers() {
      // No intermediate layers to create.
    }
  }
}