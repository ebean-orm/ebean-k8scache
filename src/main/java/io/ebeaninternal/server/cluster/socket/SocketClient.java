package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.K8sBroadcastFactory;
import io.ebeaninternal.server.cluster.message.ClusterMessage;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
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

	private static final Logger logger = K8sBroadcastFactory.log;

	private final InetSocketAddress address;

	private final String ip;

	private final String localIp;

	/**
	 * lock guarding all access
	 */
	private final ReentrantLock lock;

	private Socket socket;

	private OutputStream os;

	private DataOutputStream dataOutput;

	/**
	 * Construct with an IP address and port.
	 */
	SocketClient(String ip, InetSocketAddress address, String localIp) {
		this.lock = new ReentrantLock(false);
		this.address = address;
		this.ip = ip;
		this.localIp = localIp;
	}

	public String toString() {
		return ip;
	}

	String getIp() {
		return ip;
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
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					logger.debug("Error disconnecting from Cluster member " + localIp, e);
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
			msg.write(dataOutput);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Set whether the client is thought to be online.
	 */
	private void setOnline() throws IOException {
		connect();
	}

	private void connect() throws IOException {
		if (socket != null) {
			throw new IllegalStateException("Already got a socket connection?");
		}
		Socket s = new Socket();
		s.setKeepAlive(true);
		s.connect(address);

		this.socket = s;
		this.os = socket.getOutputStream();
		this.dataOutput = new DataOutputStream(new BufferedOutputStream(os, 512));
		sayHello();
	}

	private void sayHello() throws IOException {
		logger.debug("saying hello from local:{} to:{}", localIp, ip);
		dataOutput.writeInt(MsgKeys.HELLO);
		dataOutput.writeUTF(localIp);
		dataOutput.flush();
	}

}
