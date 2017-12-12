package io.ebeaninternal.server.cluster;

import io.ebean.config.ContainerConfig;
import io.ebeaninternal.server.cluster.socket.K8sClusterBroadcast;
import org.avaje.k8s.discovery.K8sMemberDiscovery;
import org.avaje.k8s.discovery.K8sServiceMember;
import org.avaje.k8s.discovery.K8sServiceMembers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Factory for creating the ClusterBroadcast service.
 */
public class K8sBroadcastFactory implements ClusterBroadcastFactory {

	public static final Logger log = LoggerFactory.getLogger("io.ebean.cluster.K8s");

	private static final int DEFAULT_PORT = 9911;

	@Override
  public ClusterBroadcast create(ClusterManager manager, ContainerConfig config) {

		if (!config.isActive()) {
			log.info("Cluster is not active (Refer to ContainerConfig or ebean.cluster.active)");
			return null;
		}

    if (config.getPort() == 0) {
    	config.setPort(DEFAULT_PORT);
    }

		//config.setServiceName("product-range-service");

    K8sServiceConfig k8sConfig = new K8sServiceConfig(config);
		K8sMemberDiscovery discovery = k8sConfig.getDiscovery();

		log.info("Cluster using port:{} serviceName:{} namespace:{} pod:{}", config.getPort(), discovery.getServiceName(), discovery.getNamespace(), discovery.getPodName());

		K8sServiceMember member = discovery.getMember();
		if (member == null) {
			String podName = discovery.getPodName();
			K8sServiceMembers members = discovery.getMembers();
			log.error("Unable to determine current pod Ip searching for pod:" + podName + " in members:" + members);
			return null;
		}

    return new K8sClusterBroadcast(manager, k8sConfig, member);
  }
}
