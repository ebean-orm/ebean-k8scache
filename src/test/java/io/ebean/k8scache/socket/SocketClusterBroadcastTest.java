package io.ebean.k8scache.socket;


public class SocketClusterBroadcastTest {

//  private ClusterManager mgr0;
//  private ClusterManager mgr1;
//  private TestServer server1;
//
//  SocketClusterBroadcastTest() throws InterruptedException {
//
//    ContainerConfig container0 = createContainerConfig("127.0.0.1:9901", "node0");
//    this.mgr0 = new ClusterManager(container0);
//    TestServer server0 = new TestServer("s001");
//    mgr0.registerServer(server0);
//
//    ContainerConfig container1 = createContainerConfig("127.0.0.1:9902", "node1");
//    this.mgr1 = new ClusterManager(container1);
//    this.server1 = new TestServer("s001");
//    mgr1.registerServer(server1);
//
//    Thread.sleep(100);
//  }
//
//  private ContainerConfig createContainerConfig(String local, String threadPoolName) {
//
//    ContainerConfig container0 = new ContainerConfig();
//    container0.setClusterActive(true);
//
//    Properties properties = new Properties();
//    properties.setProperty("ebean.cluster.localHostPort", local);
//    properties.setProperty("ebean.cluster.members", "127.0.0.1:9901,127.0.0.1:9902");
//    properties.setProperty("ebean.cluster.threadPoolName", threadPoolName);
//
//    container0.setProperties(properties);
//    return container0;
//  }
//
//
//  @Test
//  public void broadcast() throws Exception {
//
//    RemoteTransactionEvent evt = new RemoteTransactionEvent("s001");
//    TransactionEventTable.TableIUD tableIUD = new TransactionEventTable.TableIUD("noSuchTable", true, false, false);
//    evt.addTableIUD(tableIUD);
//
//    assertNull(server1.event);
//
//    mgr0.broadcast(evt);
//    Thread.sleep(100);
//
//    assertNotNull(server1.event);
//    assertThat(server1.event.getTableIUDList()).hasSize(1);
//
//    TransactionEventTable.TableIUD remoteTableIUD = server1.event.getTableIUDList().get(0);
//    assertEquals(remoteTableIUD.getTableName(), "noSuchTable");
//    assertEquals(remoteTableIUD.isInsert(), true);
//    assertEquals(remoteTableIUD.isUpdate(), false);
//    assertEquals(remoteTableIUD.isDelete(), false);
//  }
//
//  @AfterClass
//  public void shutdown() {
//    mgr0.shutdown();
//    mgr1.shutdown();
//  }
//
//  private class TestServer extends TDSpiEbeanServer {
//
//    RemoteTransactionEvent event;
//
//    TestServer(String name) {
//      super(name);
//    }
//
//    @Override
//    public void remoteTransactionEvent(RemoteTransactionEvent event) {
//      this.event = event;
//    }
//  }

}