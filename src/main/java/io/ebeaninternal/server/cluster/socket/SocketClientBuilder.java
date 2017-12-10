package io.ebeaninternal.server.cluster.socket;

import java.net.InetSocketAddress;

public class SocketClientBuilder {

	public static SocketClient build(String ip, int port) {
		InetSocketAddress address = new InetSocketAddress(ip, port);
		return new SocketClient(ip + ":" + port, address);
	}

	/**
	 * Parse a host:port into a InetSocketAddress.
	 */
	static SocketClient parse(String ipPort) {

		try {
			ipPort = ipPort.trim();
			int colonPos = ipPort.indexOf(':');
			if (colonPos == -1) {
				throw new IllegalArgumentException("No colon \":\" in " + ipPort);
			}
			String ip = ipPort.substring(0, colonPos);
			String sPort = ipPort.substring(colonPos + 1, ipPort.length());
			int port = Integer.parseInt(sPort);

			InetSocketAddress address = new InetSocketAddress(ip, port);
			return new SocketClient(ipPort, address);

		} catch (Exception ex) {
			throw new RuntimeException("Error parsing [" + ipPort + "] for the form [host:port]", ex);
		}
	}

}
