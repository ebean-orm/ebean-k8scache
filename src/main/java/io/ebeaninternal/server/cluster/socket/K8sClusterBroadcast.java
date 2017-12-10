package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.ClusterBroadcast;
import io.ebeaninternal.server.cluster.ClusterManager;
import io.ebeaninternal.server.cluster.K8sServiceConfig;
import io.ebeaninternal.server.cluster.message.ClusterMessage;
import io.ebeaninternal.server.cluster.message.InvalidMessageException;
import io.ebeaninternal.server.cluster.message.MessageReadWrite;
import io.ebeaninternal.server.transaction.RemoteTransactionEvent;
import org.avaje.k8s.discovery.K8sMemberDiscovery;
import org.avaje.k8s.discovery.K8sServiceMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broadcast messages across the Pods in our cluster using TCP sockets.
 */
public class K8sClusterBroadcast implements ClusterBroadcast {

	private static final Logger clusterLogger = LoggerFactory.getLogger("io.ebean.cluster");

	private static final Logger logger = LoggerFactory.getLogger(K8sClusterBroadcast.class);

	private final String podName;

	private final Map<String, SocketClient> clientMap = new ConcurrentHashMap<>();

	private final SocketClusterListener listener;

	private final MessageReadWrite messageReadWrite;

	private final AtomicLong countOutgoing = new AtomicLong();

	private final AtomicLong countIncoming = new AtomicLong();

	private final K8sServiceConfig config;

	private final int port;

	private final String ipAddress;
	private final String localIpPort;

	public K8sClusterBroadcast(ClusterManager manager, K8sServiceConfig config, K8sServiceMember member) {

		this.messageReadWrite = new MessageReadWrite(manager);
		this.config = config;
		this.port = config.getPort();
		this.ipAddress = member.getIpAddress();
		this.localIpPort = ipAddress + ":" + port;
		this.podName = member.getPodName();

		clusterLogger.info("K8 Cluster using ip:{} port:{} pod:{} ", port, ipAddress, podName);
		this.listener = new SocketClusterListener(this, port, config.getThreadPoolName());
	}

	private void checkMembership() {

		try {
			Set<String> expected = loadExpectedMembers();
			logger.error("K8 checking cluster membership against expected:{}", expected);

			for (String key : clientMap.keySet()) {
				if (!expected.contains(key)) {
					removePeer(key);
				}
			}

			for (String ipPort : expected) {
				if (!clientMap.containsKey(ipPort)) {
					registerPeer(ipPort, podName);
				}
			}
		} catch (Exception e) {
			logger.error("K8 Error during membership check", e);
		}
	}

	private Set<String> loadExpectedMembers() {

		K8sMemberDiscovery discovery = config.getDiscovery();
		discovery.reload();

		List<String> otherIps = discovery.getOtherIps();
		Set<String> expected = new HashSet<>(otherIps.size());
		for (String otherIp : otherIps) {
			expected.add(otherIp + ":" + port);
		}
		return expected;
	}

	String getIpPort() {
		return localIpPort;
	}

	/**
	 * Return the current status of this instance.
	 */
	public SocketClusterStatus getStatus() {

		int currentGroupSize = clientMap.size();
		long txnIn = countIncoming.get();
		long txnOut = countOutgoing.get();

		return new SocketClusterStatus(currentGroupSize, txnIn, txnOut);
	}

	public void startup() {
		listener.startListening();
		registerOnStartup();
	}

	public void shutdown() {
		deregister();
		listener.shutdown();
	}

	/**
	 * Register with all the other members of the Cluster.
	 */
	private void registerOnStartup() {

		ClusterMessage msg = ClusterMessage.register(localIpPort, true, podName);

		K8sMemberDiscovery discovery = config.getDiscovery();
		discovery.reload();
		List<String> otherIps = discovery.getOtherIps();

		logger.info("K8 Registering local:{} with cluster members:{}", ipAddress, otherIps);

		for (String otherIp : otherIps) {
			SocketClient member = SocketClientBuilder.build(otherIp, port);
			try {
				clusterLogger.info("K8 Register with member:{}", otherIp);
				if (member.register(msg)) {
					clusterLogger.info("K8 Registered as online with member:{}", member);
					clientMap.put(member.getIpPort(), member);
				} else {
					clusterLogger.warn("K8 Unable to register with member:{}", member);
				}
			} catch (Exception e) {
				clusterLogger.warn("K8 Unexpected error when trying to register with member:{}", member);
			}
		}
	}

	private int send(SocketClient client, ClusterMessage msg) {

		try {
			// alternative would be to connect/disconnect here but prefer to use keep alive
			if (logger.isDebugEnabled()) {
				logger.debug("K8 send to member {} broadcast msg: {}", client, msg);
			}
			client.send(msg);
			return 0;

		} catch (Exception ex) {
			logger.warn("K8 Error sending message to:" + client, ex);
			try {
				client.reconnect();
			} catch (Exception e) {
				logger.warn("K8 Error trying to reconnect to:" + client + " De-registering it.", ex);
				clientMap.remove(client.getIpPort());
			}
			return 1;
		}
	}

	private void setMemberRegister(ClusterMessage message) {
		synchronized (clientMap) {
			String ipPort = message.getRegisterHost();
			if (!message.isRegister()) {
				removePeer(ipPort);

			} else {
				SocketClient member = clientMap.get(ipPort);
				if (member != null) {
					clusterLogger.warn("K8 Cluster member [{}] already registered?", ipPort);
				} else {
					registerPeer(ipPort, message.getPodName());
				}
			}
		}
	}

	private void removePeer(String ipPort) {
		SocketClient member = clientMap.remove(ipPort);
		try {
			if (member != null) {
				clusterLogger.info("K8 Cluster member leaving [{}]", ipPort);
				member.setOnline(false);
			} else {
				clusterLogger.info("K8 Cluster member leaving [{}] but not registered?", ipPort);
			}
		} catch (Exception e) {
			clusterLogger.warn("K8 Error disconnecting from member that is leaving cluster:" + ipPort, e);
		}
	}

	private void registerPeer(String ipPort, String podName) {
		try {
			SocketClient member = SocketClientBuilder.parse(ipPort);
			member.setOnline(true);
			clientMap.put(member.getIpPort(), member);
			clusterLogger.info("K8 Cluster member joined:{} pod:{} members:{}", ipPort, podName, clientMap.keySet());
		} catch (Exception e) {
			clusterLogger.warn("K8 Error connecting to new member joining cluster:" + ipPort + " " + podName, e);
		}
	}

	/**
	 * Send the payload to all the members of the cluster.
	 */
	public void broadcast(RemoteTransactionEvent remoteTransEvent) {
		try {
			countOutgoing.incrementAndGet();
			byte[] data = messageReadWrite.write(remoteTransEvent);
			broadcast(ClusterMessage.transEvent(data));

		} catch (Exception e) {
			logger.error("K8 Error sending RemoteTransactionEvent " + remoteTransEvent + " to cluster members.", e);
		}
	}

	private void broadcast(ClusterMessage msg) {
		int errCount = 0;
		for (SocketClient member : clientMap.values()) {
			errCount += send(member, msg);
		}
		if (errCount > 0) {
			checkMembership();
		}
	}

	/**
	 * Leave the cluster.
	 */
	private void deregister() {
		clusterLogger.info("K8 Leaving cluster");
		ClusterMessage h = ClusterMessage.register(localIpPort, false, podName);
		try {
			broadcast(h);
			for (SocketClient member : clientMap.values()) {
				member.disconnect();
			}
		} catch (Exception e) {
			logger.warn("K8 Error while de-registering from cluster", e);
		}
	}

	/**
	 * Process an incoming Cluster message.
	 */
	boolean process(SocketConnection request) {

		try {
			ClusterMessage message = ClusterMessage.read(request.getDataInputStream());
			if (logger.isTraceEnabled()) {
				logger.trace("K8 ... received msg: {}", message);
			}

			if (message.isRegisterEvent()) {
				setMemberRegister(message);

			} else {
				countIncoming.incrementAndGet();
				RemoteTransactionEvent transEvent = messageReadWrite.read(message.getData());
				transEvent.run();
			}

			// instance shutting down
			return message.isRegisterEvent() && !message.isRegister();

		} catch (InterruptedIOException e) {
			logger.info("K8 Timeout waiting for message", e);
			try {
				request.disconnect();
			} catch (IOException ex) {
				logger.info("K8 Error disconnecting after timeout", ex);
			}
			return true;

		} catch (EOFException e) {
			logger.warn("K8 EOF disconnecting");
			return false;

		} catch (InvalidMessageException e) {
			//if (logger.isTraceEnabled()) {
			//}
			logger.warn("K8 invalid message key:" + e.getMessageKey());
			return false;

		} catch (IOException e) {
			logger.info("K8 IO Error waiting/reading message", e);
			return true;
		}
	}

}
