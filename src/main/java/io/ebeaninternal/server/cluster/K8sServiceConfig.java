package io.ebeaninternal.server.cluster;

import io.ebean.config.ContainerConfig;
import org.avaje.k8s.discovery.K8sMemberDiscovery;
import org.avaje.k8s.discovery.K8sServiceMember;
import org.avaje.k8s.discovery.K8sServiceMembers;

import java.util.List;

/**
 * Configuration for clustering using TCP sockets.
 */
public class K8sServiceConfig {

	private final K8sMemberDiscovery discovery;

	private String threadPoolName = "EbeanCluster";

	private int port;

	K8sServiceConfig(ContainerConfig config) {
		this.port = config.getPort();
		this.discovery = new K8sMemberDiscovery(config.getServiceName());
		discovery.setServiceName(config.getServiceName());
		discovery.setNamespace(config.getNamespace());
		discovery.setPodName(config.getPodName());
	}

	public K8sMemberDiscovery getDiscovery() {
		return discovery;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getPort() {
		return port;
	}

	/**
	 * Return the thread pool name.
	 */
	public String getThreadPoolName() {
		return threadPoolName;
	}

	/**
	 * Set the thread pool name.
	 */
	public void setThreadPoolName(String threadPoolName) {
		this.threadPoolName = threadPoolName;
	}

	public K8sServiceMember member() {
		return discovery.getMember();
	}

	public List<String> otherIps() {
		return discovery.getOtherIps();
	}

	public K8sServiceMembers allMembers() {
		return discovery.getMembers();
	}
}
