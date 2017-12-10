package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.message.ClusterMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;


/**
 * The client side of the socket clustering.
 */
class SocketClient {

  private static final Logger logger = LoggerFactory.getLogger(SocketClient.class);

  private final InetSocketAddress address;

  private final String ipPort;

  //private final String hostname;

  /**
   * lock guarding all access
   */
  private final ReentrantLock lock;

  private boolean online;

  private Socket socket;

  private OutputStream os;

  private DataOutputStream dataOutput;

  /**
   * Construct with an IP address and port.
   */
  SocketClient(String ipPort, InetSocketAddress address) {
    this.lock = new ReentrantLock(false);
    this.address = address;
    this.ipPort = ipPort;
//    this.hostname = address.getHostString();
  }

//  public String getHostname() {
//    return hostname;
//  }

  public String toString() {
    return ipPort;
  }

  String getIpPort() {
    return ipPort;
  }

  int getPort() {
    return address.getPort();
  }

  boolean isOnline() {
    return online;
  }

  void setOnline(boolean online) throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (online) {
        setOnline();
      } else {
        disconnect();
      }
    } finally {
      lock.unlock();
    }
  }

  void reconnect() throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      disconnect();
      connect();
    } finally {
      lock.unlock();
    }
  }


  void disconnect() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      this.online = false;
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          logger.info("K8 Error disconnecting from Cluster member " + ipPort, e);
        }
        os = null;
        dataOutput = null;
        socket = null;
      }
    } finally {
      lock.unlock();
    }
  }

  boolean register(ClusterMessage registerMsg) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      try {
        setOnline();
        send(registerMsg);
        return true;
      } catch (IOException e) {
        disconnect();
        return false;
      }
    } finally {
      lock.unlock();
    }
  }

  void send(ClusterMessage msg) throws IOException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (online) {
        msg.write(dataOutput);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set whether the client is thought to be online.
   */
  private void setOnline() throws IOException {
    connect();
    this.online = true;
  }

  private void connect() throws IOException {
    if (socket != null) {
      throw new IllegalStateException("K8 Already got a socket connection?");
    }
    Socket s = new Socket();
    s.setKeepAlive(true);
    s.connect(address);

    this.socket = s;
    this.os = socket.getOutputStream();
    this.dataOutput = new DataOutputStream(os);
    sayHello();
  }

  private void sayHello() throws IOException {
    logger.info("K8 Client saying hello");
    dataOutput.writeInt(MsgKeys.HELLO);
    dataOutput.flush();
  }

}
