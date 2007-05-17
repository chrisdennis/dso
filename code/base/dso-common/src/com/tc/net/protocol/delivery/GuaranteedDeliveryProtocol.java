/*
 * All content copyright (c) 2003-2006 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.net.protocol.delivery;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import com.tc.async.api.Sink;
import com.tc.net.protocol.TCNetworkMessage;
import com.tc.net.protocol.transport.MessageTransport;
import com.tc.net.protocol.transport.MessageTransportListener;

/**
 * This implements an asynchronous Once and only once protocol. Sent messages go out on the sent queue received messages
 * come in to the ProtocolMessageDelivery instance.
 */
class GuaranteedDeliveryProtocol implements DeliveryProtocol {
  private final StateMachineRunner       send;
  private final StateMachineRunner       receive;
  private final LinkedQueue              sendQueue;
  private MessageTransport transport;
  private MessageTransportListener upperLayer;

  public GuaranteedDeliveryProtocol(OOOProtocolMessageDelivery delivery, Sink workSink, LinkedQueue sendQueue) {
    this.send = new StateMachineRunner(new SendStateMachine(delivery, sendQueue), workSink);
    this.receive = new StateMachineRunner(new ReceiveStateMachine(delivery), workSink);
    this.sendQueue = sendQueue;
  }

  public void setTransport(MessageTransport t) {
    this.transport = t;
  }

  public void setUpperLayer(MessageTransportListener upperLayer) {
    this.upperLayer = upperLayer;
  }

  public void send(TCNetworkMessage message) {
    try {
      sendQueue.put(message);
      send.addEvent(new OOOProtocolEvent());
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  public void receive(OOOProtocolMessage protocolMessage) {
    if (protocolMessage.isSend() || protocolMessage.isAckRequest()) {
      receive.addEvent(new OOOProtocolEvent(protocolMessage));
    } else {
      send.addEvent(new OOOProtocolEvent(protocolMessage));
      // pass ack=-1 to receiver for re-initialize
      if (protocolMessage.getAckSequence() == -1) {
        receive.addEvent(new OOOProtocolEvent(protocolMessage));
        upperLayer.notifyTransportConnected(transport);
      }
    }
  }

  public void start() {
    send.start();
    receive.start();
  }

  public void pause() {
    send.pause();
    receive.pause();
  }

  public void resume() {
    send.resume();
    receive.resume();
  }
}