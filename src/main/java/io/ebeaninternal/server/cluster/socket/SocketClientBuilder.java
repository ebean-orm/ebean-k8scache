package io.ebeaninternal.server.cluster.socket;

import java.net.InetSocketAddress;

public class SocketClientBuilder {

	private final String localIp;

	SocketClientBuilder(String localIp) {
		this.localIp = localIp;
	}

	public SocketClient build(String ip, int port) {
		InetSocketAddress address = new InetSocketAddress(ip, port);
		return new SocketClient(ip, address, localIp);
	}

}
