package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.K8sBroadcastFactory;
import io.ebeaninternal.server.executor.DaemonThreadFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Server side multi-threaded socket listener. Accepts connections and dispatches
 * them to an appropriate handler.
 * <p>
 * This is designed as a single port listener, where part of the connection
 * protocol determines which service the client is requesting (rather than a
 * port per service).
 * </p>
 * <p>
 * It has its own daemon background thread that handles the accept() loop on the
 * ServerSocket.
 * </p>
 */
class SocketClusterListener implements Runnable {

  private static final Logger log = K8sBroadcastFactory.log;

  /**
   * The server socket used to listen for requests.
   */
  private final ServerSocket serverListenSocket;

  /**
   * The listening thread.
   */
  private final Thread listenerThread;

  /**
   * The pool of threads that actually do the parsing execution of requests.
   */
  private final ExecutorService service;

  private final K8sClusterBroadcast owner;

  /**
   * shutting down flag.
   */
  private boolean doingShutdown;

  /**
   * Whether the listening thread is busy assigning a request to a thread.
   */
  private volatile boolean isActive;

  /**
   * Construct with a given thread pool name.
   */
  SocketClusterListener(K8sClusterBroadcast owner, int port, String poolName) {
    this.owner = owner;
    this.service = Executors.newCachedThreadPool(new DaemonThreadFactory(poolName));
    try {
      this.serverListenSocket = new ServerSocket(port);
      this.serverListenSocket.setSoTimeout(60000);
      this.listenerThread = new Thread(this, "EbeanClusterListener");

    } catch (IOException e) {
      throw new RuntimeException("Error starting cluster socket listener on port " + port, e);
    }
  }

  /**
   * Start listening for requests.
   */
  public void startListening() {
    log.trace("startListening");
    this.listenerThread.setDaemon(true);
    this.listenerThread.start();
  }

  /**
   * Shutdown this listener.
   */
  public void shutdown() {
    doingShutdown = true;
    try {
      if (isActive) {
        synchronized (listenerThread) {
          try {
            listenerThread.wait(1000);
          } catch (InterruptedException e) {
            // OK to ignore as expected to Interrupt for shutdown.
          }
        }
      }
      listenerThread.interrupt();
      serverListenSocket.close();
    } catch (IOException e) {
      log.error("Error shutting down listener", e);
    }

    service.shutdown();
  }

  /**
   * This is a runnable and so this must be public. Don't call this externally
   * but rather call the startListening() method.
   */
  public void run() {
    // run in loop until doingShutdown is true...
    while (!doingShutdown) {
      try {
        synchronized (listenerThread) {
          Socket clientSocket = serverListenSocket.accept();
          isActive = true;
          service.execute(new RequestProcessor(owner, clientSocket));
          isActive = false;
        }
      } catch (SocketException e) {
        if (doingShutdown) {
          log.debug("Doing shutdown and accept threw:" + e.getMessage());
        } else {
          log.error("Error while listening", e);
        }
      } catch (InterruptedIOException e) {
        // this will happen when the server is quiet
        log.debug("Expected due to accept timeout? {}", e.getMessage());

      } catch (IOException e) {
        // log it and continue in the loop...
        log.error("IOException processing cluster message", e);
      }
    }
  }

}
