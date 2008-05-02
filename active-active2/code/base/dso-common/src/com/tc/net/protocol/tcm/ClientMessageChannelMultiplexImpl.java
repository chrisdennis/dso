/*
 * All content copyright (c) 2003-2007 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.net.protocol.tcm;

import com.tc.logging.TCLogger;
import com.tc.logging.TCLogging;
import com.tc.net.MaxConnectionsExceededException;
import com.tc.net.core.ConnectionAddressProvider;
import com.tc.net.groups.NodeID;
import com.tc.net.groups.NodeIDImpl;
import com.tc.net.protocol.NetworkStackID;
import com.tc.net.protocol.transport.MessageTransport;
import com.tc.net.protocol.transport.TransportHandshakeMessage;
import com.tc.object.session.SessionProvider;
import com.tc.util.TCTimeoutException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;

public class ClientMessageChannelMultiplexImpl extends ClientMessageChannelImpl implements
    ClientMessageChannelMultiplex {
  private static final TCLogger       logger = TCLogging.getLogger(ClientMessageChannelMultiplex.class);
  private final TCMessageFactory      msgFactory;
  private final SessionProvider       sessionProvider;

  private CommunicationsManager       communicationsManager;
  private ConnectionAddressProvider[] addressProviders;
  private ClientMessageChannel[]      channels;
  private NodeID[]                    nodeIDs;

  public ClientMessageChannelMultiplexImpl(TCMessageFactory msgFactory,
                                           SessionProvider sessionProvider,
                                           final int maxReconnectTries,
                                           CommunicationsManager communicationsManager,
                                           ConnectionAddressProvider[] addressProviders) {
    super(msgFactory, null, sessionProvider, null, null, false);
    this.msgFactory = msgFactory;
    this.sessionProvider = sessionProvider;

    this.communicationsManager = communicationsManager;
    this.addressProviders = addressProviders;
    this.channels = new ClientMessageChannel[addressProviders.length];
    this.nodeIDs = new NodeID[addressProviders.length];

    for (int i = 0; i < addressProviders.length; ++i) {
      boolean isActiveCoordinator = (i == 0);
      channels[i] = this.communicationsManager.createClientChannel(this.sessionProvider, -1, null, 0, 10000,
                                                                   this.addressProviders[i], 
                                                                   TransportHandshakeMessage.NO_CALLBACK_PORT, null,
                                                                   this.msgFactory,
                                                                   new TCMessageRouterImpl(), this, isActiveCoordinator);
    }
  }

  public ClientMessageChannel getActiveCoordinator() {
    return channels[0];
  }

  public ChannelID getActiveActiveChannelID() {
    return getActiveCoordinator().getChannelID();
  }

  public NodeID makeNodeMultiplexId(ChannelID cid, ConnectionAddressProvider addressProvider) {
    // XXX ....
    return (new NodeIDImpl(addressProvider + cid.toString(), addressProvider.toString().getBytes()));
  }

  public ClientMessageChannel[] getChannels() {
    return (channels);
  }

  public NodeID[] getMultiplexIDs() {
    return (nodeIDs);
  }

  public ClientMessageChannel getChannel(NodeID id) {
    for (int i = 0; i < nodeIDs.length; ++i) {
      if (id.equals(nodeIDs[i])) { 
        return (channels[i]); }
    }
    return null;
  }

  public void broadcast(final TCMessage[] messages) {
    for (int i = 0; i < messages.length; ++i) {
      messages[i].send();
    }
  }
  
  public TCMessage[] createBroadcastMessage(TCMessageType type) {
    TCMessage[] msgs = new TCMessage[channels.length];
    for(int i =0; i < channels.length; ++i) {
      msgs[i] = msgFactory.createMessage(channels[i], type);
    }
    return (msgs);
  }

  public TCMessage createMessage(NodeID id, TCMessageType type) {
    TCMessage rv = msgFactory.createMessage(getChannel(id), type);
    return rv;
  }

  public TCMessage createMessage(TCMessageType type) {
    TCMessage rv = msgFactory.createMessage(getChannels()[0], type);
    return rv;
  }

  public NetworkStackID open() throws TCTimeoutException, UnknownHostException, IOException,
      MaxConnectionsExceededException {
    NetworkStackID nid = null;
    for (int i = 0; i < channels.length; ++i) {
      nid = channels[i].open();
      // XXX EY more work here to setup NodeID
      nodeIDs[i] = makeNodeMultiplexId(channels[0].getChannelID(), this.addressProviders[i]);
      channels[i].setDestinationNodeID(nodeIDs[i]);
      // XXX EY set source NodeID
    }
    return nid;
  }

  public ChannelID getChannelID() {
    // return one of active-coordinator, they are same for all channels
    return getActiveCoordinator().getChannelID();
  }

  public int getConnectCount() {
    // an aggregate of all channels
    int count = 0;
    for (int i = 0; i < channels.length; ++i)
      count += channels[i].getConnectCount();
    return count;
  }

  public int getConnectAttemptCount() {
    // an aggregate of all channels
    int count = 0;
    for (int i = 0; i < channels.length; ++i)
      count += channels[i].getConnectAttemptCount();
    return count;
  }
  
  public void routeMessageType(TCMessageType messageType, TCMessageSink dest) {
    for (int i = 0; i < channels.length; ++i)
      channels[i].routeMessageType(messageType, dest);
  }


  public void notifyTransportConnected(MessageTransport transport) {
    throw new AssertionError();
  }

  public void notifyTransportDisconnected(MessageTransport transport) {
    throw new AssertionError();
  }

  public void notifyTransportConnectAttempt(MessageTransport transport) {
    throw new AssertionError();
  }

  public void notifyTransportClosed(MessageTransport transport) {
    throw new AssertionError();
  }

  public ChannelIDProvider getChannelIDProvider() {
    // return one from active-coordinator
    return getActiveCoordinator().getChannelIDProvider();
  }

  public void close() {
    for (int i = 0; i < channels.length; ++i)
      channels[i].close();
  }

  public boolean isConnected() {
    if (channels.length == 0) return false;
    for (int i = 0; i < channels.length; ++i) {
      if (!channels[i].isConnected()) return false;
    }
    return true;
  }

  public boolean isOpen() {
    if (channels.length == 0) return false;
    for (int i = 0; i < channels.length; ++i) {
      if (!channels[i].isOpen()) return false;
    }
    return true;
  }

  public ClientMessageChannel channel() {
    // return the active-coordinator
    return getActiveCoordinator();
  }

  /*
   * As a middleman between ClientHandshakeManager and multiple ClientMessageChannels. Bookkeeping sub-channels' events
   * Notify connected only when all channel connected. Notify disconnected when any channel disconnected Notify closed
   * when any channel closed
   */
  private class ChannelEventMiddleMan implements ChannelEventListener {
    private final ChannelEventListener listener;
    private HashSet                    connectedSet = new HashSet();

    public ChannelEventMiddleMan(ChannelEventListener listener) {
      this.listener = listener;
    }

    public void notifyChannelEvent(ChannelEvent event) {
      if (event.getType() == ChannelEventType.TRANSPORT_DISCONNECTED_EVENT) {
        if (connectedSet.remove(event.getChannel())) {
          fireEvent(event);
        }
      } else if (event.getType() == ChannelEventType.TRANSPORT_CONNECTED_EVENT) {
        connectedSet.add(event.getChannel());
        if (connectedSet.size() == channels.length) {
          fireEvent(event);
        }
      } else if (event.getType() == ChannelEventType.CHANNEL_CLOSED_EVENT) {
        if (connectedSet.remove(event.getChannel())) {
          fireEvent(event);
        }
      }
    }

    private void fireEvent(ChannelEvent event) {
      listener.notifyChannelEvent(event);
    }
  }

  public void addListener(ChannelEventListener listener) {
    if (channels.length == 1) {
      channels[0].addListener(listener);
      return;
    }

    ChannelEventMiddleMan middleman = new ChannelEventMiddleMan(listener);
    for (int i = 0; i < channels.length; ++i)
      channels[i].addListener(middleman);
  }

}