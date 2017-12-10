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

	private static final Logger log = LoggerFactory.getLogger(K8sBroadcastFactory.class);

	private static final int DEFAULT_PORT = 9911;

	@Override
  public ClusterBroadcast create(ClusterManager manager, ContainerConfig config) {

    if (config.getPort() == 0) {
    	config.setPort(DEFAULT_PORT);
    }

		config.setServiceName("product-range-service");

    K8sServiceConfig k8sConfig = new K8sServiceConfig(config);
		K8sMemberDiscovery discovery = k8sConfig.getDiscovery();
		K8sServiceMember member = discovery.getMember();

		if (member == null) {
			String searchedPodName = discovery.getPodName();
			K8sServiceMembers members = discovery.getMembers();
			log.error("K8 Unable to determine current pod Ip searching for[" + searchedPodName + "] in AllMembers:" + members);
			return null;
		}


		try {
      return new K8sClusterBroadcast(manager, k8sConfig, member);

    } catch (IllegalStateException e) {
      log.error("K8 Failed to create kubernetes aware cluster", e);
      return null;
    }
  }
}
