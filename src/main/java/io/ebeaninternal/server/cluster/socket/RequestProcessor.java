package io.ebeaninternal.server.cluster.socket;

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

  private static final Logger logger = LoggerFactory.getLogger(RequestProcessor.class);

  private final Socket clientSocket;

  private final K8sClusterBroadcast owner;

  private final String hostPort;

  /**
   * Create including the Listener (used to lookup the Request Handler) and
   * the socket itself.
   */
  RequestProcessor(K8sClusterBroadcast owner, Socket clientSocket) {
    this.clientSocket = clientSocket;
    this.owner = owner;
    this.hostPort = owner.getIpPort();
  }

  /**
   * This will parse out the command.  Lookup the appropriate Handler and
   * pass the information to the handler for processing.
   * <P>Dev Note: the command parsing is processed here so that it is preformed
   * by the assigned thread rather than the listeners thread.</P>
   */
  public void run() {
    try {
      logger.info("K8 start listening for cluster messages");
      SocketConnection sc = new SocketConnection(clientSocket);
      DataInputStream dataInputStream = sc.getDataInputStream();
      int helloKey = dataInputStream.readInt();
      if (helloKey != MsgKeys.HELLO) {
        logger.warn("K8 Received Invalid hello {}", helloKey);
      } else {
        logger.info("K8 Received hello, reading messages");
        while (true) {
          if (owner.process(sc)) {
            // got the offline message or timeout
            break;
          }
        }
      }
      logger.info("K8 disconnecting: {}", hostPort);
      sc.disconnect();

    } catch (Exception e) {
      logger.error("K8 Error listening for messages - " + owner.getIpPort(), e);
    }
  }

}