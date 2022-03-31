module io.ebean.k8scache {

    exports io.ebean.k8scache;

    requires io.ebean.core;
    requires k8s.discovery;

    provides io.ebeaninternal.server.cluster.ClusterBroadcastFactory with io.ebean.k8scache.K8sBroadcastFactory;
}