/*
 * All content copyright (c) 2003-2007 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.object.lockmanager.impl;

import com.tc.async.api.Sink;
import com.tc.exception.ImplementMe;
import com.tc.io.TCByteBufferOutput;
import com.tc.io.TCByteBufferOutputStream;
import com.tc.logging.NullTCLogger;
import com.tc.management.ClientLockStatManager;
import com.tc.management.ClientLockStatManagerImpl;
import com.tc.management.L2LockStatsManager;
import com.tc.management.L2LockStatsManagerImpl;
import com.tc.management.L2LockStatsManagerImpl.LockStackTracesStat;
import com.tc.net.MaxConnectionsExceededException;
import com.tc.net.groups.ClientID;
import com.tc.net.protocol.tcm.ChannelEventListener;
import com.tc.net.protocol.tcm.ChannelID;
import com.tc.net.protocol.tcm.ChannelIDProvider;
import com.tc.net.protocol.tcm.ClientMessageChannel;
import com.tc.net.protocol.tcm.MessageChannel;
import com.tc.net.protocol.tcm.MessageMonitor;
import com.tc.net.protocol.tcm.MockMessageChannel;
import com.tc.net.protocol.tcm.NullMessageMonitor;
import com.tc.net.protocol.tcm.TCMessage;
import com.tc.net.protocol.tcm.TCMessageImpl;
import com.tc.net.protocol.tcm.TCMessageSink;
import com.tc.net.protocol.tcm.TCMessageType;
import com.tc.object.lockmanager.api.LockID;
import com.tc.object.lockmanager.api.LockLevel;
import com.tc.object.lockmanager.api.ThreadID;
import com.tc.object.msg.AcknowledgeTransactionMessageFactory;
import com.tc.object.msg.ClientHandshakeMessageFactory;
import com.tc.object.msg.CommitTransactionMessageFactory;
import com.tc.object.msg.JMXMessage;
import com.tc.object.msg.LockRequestMessageFactory;
import com.tc.object.msg.LockStatisticsResponseMessage;
import com.tc.object.msg.ObjectIDBatchRequestMessageFactory;
import com.tc.object.msg.RequestManagedObjectMessageFactory;
import com.tc.object.msg.RequestRootMessageFactory;
import com.tc.object.net.DSOChannelManager;
import com.tc.object.net.DSOClientMessageChannel;
import com.tc.object.session.SessionID;
import com.tc.object.session.TestSessionManager;
import com.tc.objectserver.api.TestSink;
import com.tc.objectserver.lockmanager.api.NullChannelManager;
import com.tc.objectserver.lockmanager.impl.LockManagerImpl;
import com.tc.util.Assert;
import com.tc.util.TCTimeoutException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;

import junit.framework.TestCase;

public class ClientServerLockStatisticsTest extends TestCase {

  private ClientLockManagerImpl           clientLockManager;
  private LockManagerImpl                 serverLockManager;
  private ClientServerLockManagerGlue     glue1;
  private TestSessionManager              sessionManager;
  private ClientLockStatManager           clientLockStatManager;
  private L2LockStatsManager              serverLockStatManager;
  private ClientServerLockStatManagerGlue statGlue;
  private ChannelID channelId1 = new ChannelID(1);

  protected void setUp() throws Exception {
    super.setUp();
    sessionManager = new TestSessionManager();
    glue1 = new ClientServerLockManagerGlue(sessionManager);
    clientLockStatManager = new ClientLockStatManagerImpl();
    clientLockManager = new ClientLockManagerImpl(new NullTCLogger(), glue1, sessionManager, clientLockStatManager);

    DSOChannelManager nullChannelManager = new NullChannelManager();
    serverLockStatManager = new L2LockStatsManagerImpl();
    serverLockManager = new LockManagerImpl(nullChannelManager, serverLockStatManager);
    serverLockManager.setLockPolicy(LockManagerImpl.ALTRUISTIC_LOCK_POLICY);
    glue1.set(clientLockManager, serverLockManager);

    TestSink sink = new TestSink();
    serverLockStatManager.start(nullChannelManager, serverLockManager, sink);

    ClientMessageChannel channel1 = new TestClientMessageChannel(channelId1);

    clientLockStatManager.start(new TestClientChannel(channel1), sink);
    statGlue = new ClientServerLockStatManagerGlue(sink);
    statGlue.set(clientLockStatManager, serverLockStatManager);
  }

  public void testCollectLockStackTraces() {
    final LockID lockID1 = new LockID("1");
    final ThreadID tx1 = new ThreadID(1);
    final ThreadID tx2 = new ThreadID(1);
    clientLockManager.lock(lockID1, tx1, LockLevel.READ);
    serverLockStatManager.enableClientStat(lockID1);

    for (int i=0; i<10; i++) {
      clientLockManager.lock(lockID1, tx2, LockLevel.READ);
      clientLockManager.unlock(lockID1, tx2);
    }

    sleep(2000);
    Collection stackTraces = serverLockStatManager.getStackTraces(lockID1);
    
    Assert.assertEquals(1, stackTraces.size());
    for (Iterator i=stackTraces.iterator(); i.hasNext(); ) {
      LockStackTracesStat s = (LockStackTracesStat)i.next();
      Assert.assertEquals(channelId1, ((ClientID)s.getNodeID()).getChannelID());
      // 10 stacktraces for lock,  and 10 for unlock.
      Assert.assertEquals(20, s.getStackTraces().size());
    }
  }

  private void sleep(long l) {
    try {
      Thread.sleep(l);
    } catch (InterruptedException e) {
      // NOP
    }
  }

  protected void tearDown() throws Exception {
    glue1.stop();
    super.tearDown();
  }

  private static class TestClientMessageChannel extends MockMessageChannel implements ClientMessageChannel {
    public TestClientMessageChannel(ChannelID channelId) {
      super(channelId);
      super.registerType(TCMessageType.LOCK_STATISTICS_RESPONSE_MESSAGE, LockStatisticsResponseMessage.class);
    }

    public TCMessage createMessage(TCMessageType type) {
      Class theClass = super.getRegisteredMessageClass(type);

      if (theClass == null) throw new ImplementMe();

      try {
        Constructor constructor = theClass.getConstructor(new Class[] { SessionID.class, MessageMonitor.class,
            TCByteBufferOutput.class, MessageChannel.class, TCMessageType.class });
        TCMessageImpl message = (TCMessageImpl) constructor.newInstance(new Object[] { SessionID.NULL_ID, new NullMessageMonitor(),
            new TCByteBufferOutputStream(4, 4096, false), this, type });
        message.seal();
        return message;
      } catch (Exception e) {
        throw new ImplementMe("Failed", e);
      }
    }

    public void addClassMapping(TCMessageType type, Class msgClass) {
      throw new ImplementMe();

    }

    public ChannelIDProvider getChannelIDProvider() {
      return null;
    }

    public int getConnectAttemptCount() {
      return 0;
    }

    public int getConnectCount() {
      return 0;
    }

    public void routeMessageType(TCMessageType messageType, Sink destSink, Sink hydrateSink) {
      throw new ImplementMe();

    }

    public void routeMessageType(TCMessageType type, TCMessageSink sink) {
      throw new ImplementMe();

    }

    public void unrouteMessageType(TCMessageType type) {
      throw new ImplementMe();

    }
  }

  private static class TestClientChannel implements DSOClientMessageChannel {
    private ClientMessageChannel clientMessageChannel;

    public TestClientChannel(ClientMessageChannel clientMessageChannel) {
      this.clientMessageChannel = clientMessageChannel;
    }

    public ClientMessageChannel channel() {
      return clientMessageChannel;
    }

    public void addClassMapping(TCMessageType messageType, Class messageClass) {
      throw new ImplementMe();
      
    }

    public void addListener(ChannelEventListener listener) {
      throw new ImplementMe();
      
    }

    public void close() {
      throw new ImplementMe();
      
    }

    public AcknowledgeTransactionMessageFactory getAcknowledgeTransactionMessageFactory() {
      throw new ImplementMe();
    }

    public ChannelIDProvider getChannelIDProvider() {
      throw new ImplementMe();
    }

    public ClientHandshakeMessageFactory getClientHandshakeMessageFactory() {
      throw new ImplementMe();
    }

    public CommitTransactionMessageFactory getCommitTransactionMessageFactory() {
      throw new ImplementMe();
    }

    public JMXMessage getJMXMessage() {
      throw new ImplementMe();
    }

    public LockRequestMessageFactory getLockRequestMessageFactory() {
      throw new ImplementMe();
    }

    public ObjectIDBatchRequestMessageFactory getObjectIDBatchRequestMessageFactory() {
      throw new ImplementMe();
    }

    public RequestManagedObjectMessageFactory getRequestManagedObjectMessageFactory() {
      throw new ImplementMe();
    }

    public RequestRootMessageFactory getRequestRootMessageFactory() {
      throw new ImplementMe();
    }

    public boolean isConnected() {
      throw new ImplementMe();
    }

    public void open() throws MaxConnectionsExceededException, TCTimeoutException, UnknownHostException, IOException {
      throw new ImplementMe();
      
    }

    public void routeMessageType(TCMessageType messageType, Sink destSink, Sink hydrateSink) {
      throw new ImplementMe();
      
    }
  }
}
