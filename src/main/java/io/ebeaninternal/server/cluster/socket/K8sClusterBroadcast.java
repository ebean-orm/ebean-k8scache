package io.ebeaninternal.server.cluster.socket;

import io.ebeaninternal.server.cluster.ClusterBroadcast;
import io.ebeaninternal.server.cluster.ClusterManager;
import io.ebeaninternal.server.cluster.K8sBroadcastFactory;
import io.ebeaninternal.server.cluster.K8sServiceConfig;
import io.ebeaninternal.server.cluster.message.ClusterMessage;
import io.ebeaninternal.server.cluster.message.InvalidMessageException;
import io.ebeaninternal.server.cluster.message.MessageReadWrite;
import io.ebeaninternal.server.transaction.RemoteTransactionEvent;
import org.avaje.k8s.discovery.K8sMemberDiscovery;
import org.avaje.k8s.discovery.K8sServiceMember;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broadcast messages across the Pods in our cluster using TCP sockets.
 */
public class K8sClusterBroadcast implements ClusterBroadcast {

	private static final Logger log = K8sBroadcastFactory.log;

	private static final long normalFreqMillis = 300_000;

	private static final long errorFreqMillis = 60_000;

	private final String podName;

	private final Map<String, SocketClient> members = new ConcurrentHashMap<>();

	private final SocketClusterListener listener;

	private final MessageReadWrite messageReadWrite;

	private final AtomicLong countOutgoing = new AtomicLong();

	private final AtomicLong countIncoming = new AtomicLong();

	private final K8sServiceConfig config;

	private final SocketClientBuilder clientBuilder;

	private final int port;

	private final String localIp;

	private final ClusterMessage registerMessage;

	private volatile long checkStatus;

	private final AtomicLong errorCount = new AtomicLong();

	public K8sClusterBroadcast(ClusterManager manager, K8sServiceConfig config, K8sServiceMember member) {

		this.messageReadWrite = new MessageReadWrite(manager);
		this.config = config;
		this.port = config.getPort();
		this.localIp = member.getIpAddress();
		this.podName = member.getPodName();

		K8sMemberDiscovery discovery = config.getDiscovery();
		log.info("Cluster using localIp:{} port:{} serviceName:{} namespace:{} pod:{}", localIp, port, discovery.getServiceName(), discovery.getNamespace(), podName);

		this.clientBuilder = new SocketClientBuilder(localIp);
		this.registerMessage = ClusterMessage.register(localIp, true, podName);
		this.listener = new SocketClusterListener(this, port, config.getThreadPoolName());
	}

	String getLocalIp() {
		return localIp;
	}

	/**
	 * Called to indicate a membership should run shortly.
	 */
	void checkStatus(boolean hadError) {

		if (hadError) {
			errorCount.incrementAndGet();
		}
		long lastCheck = System.currentTimeMillis() - checkStatus;
		if (lastCheck > normalFreqMillis) {
			checkMembership();
		} else if (errorCount.get() > 0 && lastCheck > errorFreqMillis) {
			checkMembership();
		}
	}

	private synchronized void checkMembership() {

		try {
			Set<String> expected = loadExpectedMembers();
			checkStatus = System.currentTimeMillis();
			log.debug("check membership - expected:{} current:{}", expected, members.keySet());
			for (String key : members.keySet()) {
				if (!expected.contains(key)) {
					removePeer(key);
				}
			}

			for (String otherId : expected) {
				if (!members.containsKey(otherId)) {
					registerPeer(otherId, podName);
				}
			}

			errorCount.set(0);

		} catch (Exception e) {
			log.error("Error during membership check", e);
		}
	}

	private Set<String> loadExpectedMembers() {

		K8sMemberDiscovery discovery = config.getDiscovery();
		discovery.reload();
		return new LinkedHashSet<>(discovery.getOtherIps());
	}

	/**
	 * Return the current status of this instance.
	 */
	public SocketClusterStatus getStatus() {
		return new SocketClusterStatus(members.size(), countIncoming.get(), countOutgoing.get());
	}

	public void startup() {
		listener.startListening();
		checkMembership();
	}

	public void shutdown() {
		deregister();
		listener.shutdown();
	}

	private int send(SocketClient client, ClusterMessage msg) {

		try {
			// alternative would be to connect/disconnect here but prefer to use keep alive
			if (log.isTraceEnabled()) {
				log.trace("send to member {} broadcast msg: {}", client, msg);
			}
			client.send(msg);
			return 0;

		} catch (IOException ex) {
			log.warn("reconnect due to error sending message to:" + client, ex);
			try {
				client.reconnect();
			} catch (Exception e) {
				log.warn("Error trying to reconnect to:" + client + " De-registering it.", ex);
				members.remove(client.getIp());
			}
			return 1;
		}
	}

	private void setMemberRegister(ClusterMessage message) {
		String ipPort = message.getRegisterIp();
		if (!message.isRegister()) {
			removePeer(ipPort);

		} else {
			SocketClient member = members.get(ipPort);
			if (member != null) {
				log.warn("Cluster member [{}] already registered?", ipPort);
			} else {
				registerPeer(ipPort, message.getPodName());
			}
		}
		//checkMembership();
	}

	private void removePeer(String ipPort) {
		SocketClient member = members.remove(ipPort);
		try {
			if (member != null) {
				log.debug("member leaving [{}]", ipPort);
				member.disconnect();
			} else {
				log.info("cluster member leaving [{}] but not registered?", ipPort);
			}
		} catch (Exception e) {
			log.warn("Error disconnecting from member that is leaving cluster:" + ipPort, e);
		}
	}

	private void registerPeer(String otherIp, String podName) {
		try {
			SocketClient member = clientBuilder.build(otherIp, port);
			if (member.register(registerMessage)) {
				log.debug("Registered with member:{}", otherIp);
				members.put(member.getIp(), member);
			} else {
				log.warn("Unable to register with member:{}", member);
			}
		} catch (Exception e) {
			log.warn("Error connecting to new member joining cluster:" + otherIp + " " + podName, e);
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
			log.error("Error sending RemoteTransactionEvent " + remoteTransEvent + " to cluster members.", e);
		}
	}

	private void broadcast(ClusterMessage msg) {
		int errCount = 0;
		for (SocketClient member : members.values()) {
			errCount += send(member, msg);
		}
		if (errCount > 0) {
			log.debug("broadcast errors:{}", errCount);
			checkStatus(true);
		}
	}

	/**
	 * Leave the cluster.
	 */
	private void deregister() {
		log.info("Leaving cluster");
		ClusterMessage h = ClusterMessage.register(localIp, false, podName);
		try {
			broadcast(h);
			for (SocketClient member : members.values()) {
				member.disconnect();
			}
		} catch (Exception e) {
			log.warn("Error while de-registering from cluster", e);
		}
	}

	/**
	 * Process an message return true if done and should disconnect.
	 */
	boolean process(SocketConnection request) {

		try {
			ClusterMessage message = ClusterMessage.read(request.getDataInputStream());
			if (log.isTraceEnabled()) {
				log.trace("received msg: {}", message);
			}

			if (message.isRegisterEvent()) {
				setMemberRegister(message);

			} else {
				countIncoming.incrementAndGet();
				RemoteTransactionEvent event = messageReadWrite.read(message.getData());
				if (log.isTraceEnabled()) {
					log.trace("event:{}", event);
				}
				event.run();
			}

			// return true of a de-register event
			return message.isRegisterEvent() && !message.isRegister();

		} catch (InterruptedIOException e) {
			log.info("Timeout waiting for message", e);
			try {
				request.disconnect();
			} catch (IOException ex) {
				log.info("Error disconnecting after timeout", ex);
			}
			return true;

		} catch (EOFException e) {
			log.debug("EOF disconnecting");
			return true;

		} catch (InvalidMessageException e) {
			log.warn(e.getMessage());
			return true;

		} catch (IOException e) {
			log.info("IO Error waiting/reading message", e);
			return true;
		}
	}

}
