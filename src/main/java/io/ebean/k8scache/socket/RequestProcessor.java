package io.ebean.k8scache.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.net.Socket;

/**
 * This parses and dispatches a request to the appropriate handler.
 * <p>
 * Looks up the appropriate RequestHandler
 * and then gets it to process the Client request.<P>
 * </p>
 * Note that this is a Runnable because it is assigned to the ThreadPool.
 */
class RequestProcessor implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(RequestProcessor.class);

  private final Socket clientSocket;

  private final K8sClusterBroadcast owner;

  private final String localIp;

  /**
   * Create including the Listener (used to lookup the Request Handler) and
   * the socket itself.
   */
  RequestProcessor(K8sClusterBroadcast owner, Socket clientSocket) {
    this.clientSocket = clientSocket;
    this.owner = owner;
    this.localIp = owner.getLocalIp();
  }

  /**
   * This will parse out the command.  Lookup the appropriate Handler and
   * pass the information to the handler for processing.
   * <P>Dev Note: the command parsing is processed here so that it is preformed
   * by the assigned thread rather than the listeners thread.</P>
   */
  public void run() {
    try {
      SocketConnection sc = new SocketConnection(clientSocket);
      DataInputStream dataInputStream = sc.getDataInputStream();
      int helloKey = dataInputStream.readInt();
      if (helloKey != MsgKeys.HELLO) {
        if (log.isTraceEnabled()) {
          log.trace("Received Invalid hello {} from {}", helloKey, clientSocket.getRemoteSocketAddress());
        }
      } else {
	      String fromMember = dataInputStream.readUTF();
	      if (log.isDebugEnabled()) {
          log.debug("reading messages from:{} sa:{}", fromMember, clientSocket.getRemoteSocketAddress());
        }
        while (true) {
          if (owner.process(sc)) {
            // got the offline message or timeout
            break;
          }
        }
        if (log.isDebugEnabled()) {
          log.debug("end of reading messages from:{} sa:{}", fromMember, clientSocket.getRemoteSocketAddress());
        }
      }
      if (log.isTraceEnabled()) {
        log.trace("disconnecting client {}", clientSocket.getRemoteSocketAddress());
      }
      sc.disconnect();

      owner.checkStatus(false);

    } catch (Exception e) {
      log.error("Error listening for messages - " + localIp, e);
    }
  }

}