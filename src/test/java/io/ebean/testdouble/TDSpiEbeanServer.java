package io.ebean.testdouble;

/**
 * Test double for SpiEbeanServer.
 */
public class TDSpiEbeanServer {//extends TDEbeanServer implements SpiEbeanServer {
//
//  String name;
//
//  public TDSpiEbeanServer(String name) {
//    this.name = name;
//  }
//
//  @Override
//  public void shutdownManaged() {
//
//  }
//
//  @Override
//  public Object currentTenantId() {
//    return null;
//  }
//
//  @Override
//  public SpiTransaction createQueryTransaction(Object tenantId) {
//    return null;
//  }
//
//  @Override
//  public boolean isCollectQueryOrigins() {
//    return false;
//  }
//
//  @Override
//  public boolean isUpdateAllPropertiesInBatch() {
//    return false;
//  }
//
//  @Override
//  public ServerConfig getServerConfig() {
//    return null;
//  }
//
//  @Override
//  public DatabasePlatform getDatabasePlatform() {
//    return null;
//  }
//
//  @Override
//  public CallStack createCallStack() {
//    return null;
//  }
//
//  @Override
//  public PersistenceContextScope getPersistenceContextScope(SpiQuery<?> query) {
//    return null;
//  }
//
//  @Override
//  public DataTimeZone getDataTimeZone() {
//    return null;
//  }
//
//  @Override
//  public ReadAuditLogger getReadAuditLogger() {
//    return null;
//  }
//
//  @Override
//  public ReadAuditPrepare getReadAuditPrepare() {
//    return null;
//  }
//
//
//  @Override
//  public void clearQueryStatistics() {
//
//  }
//
//  @Override
//  public BeanDescriptor<?> getBeanDescriptorByQueueId(String queueId) {
//    return null;
//  }
//
//  @Override
//  public List<BeanDescriptor<?>> getBeanDescriptors() {
//    return null;
//  }
//
//  @Override
//  public <T> BeanDescriptor<T> getBeanDescriptor(Class<T> type) {
//    return null;
//  }
//
//  @Override
//  public BeanDescriptor<?> getBeanDescriptorById(String descriptorId) {
//    return null;
//  }
//
//  @Override
//  public List<BeanDescriptor<?>> getBeanDescriptors(String tableName) {
//    return null;
//  }
//
//  @Override
//  public void externalModification(TransactionEventTable event) {
//
//  }
//
//  @Override
//  public SpiTransaction createServerTransaction(boolean isExplicit, int isolationLevel) {
//    return null;
//  }
//
//  @Override
//  public SpiTransaction getCurrentServerTransaction() {
//    return null;
//  }
//
//  @Override
//  public ScopeTrans createScopeTrans(TxScope txScope) {
//    return null;
//  }
//
//  @Override
//  public void remoteTransactionEvent(RemoteTransactionEvent event) {
//
//  }
//
//  @Override
//  public <T> CQuery<T> compileQuery(Query<T> query, Transaction t) {
//    return null;
//  }
//
//  @Override
//  public <A, T> List<A> findIdsWithCopy(Query<T> query, Transaction t) {
//    return null;
//  }
//
//  @Override
//  public <T> int findRowCountWithCopy(Query<T> query, Transaction t) {
//    return 0;
//  }
//
//  @Override
//  public void loadBean(LoadBeanRequest loadRequest) {
//
//  }
//
//  @Override
//  public void loadMany(LoadManyRequest loadRequest) {
//
//  }
//
//  @Override
//  public int getLazyLoadBatchSize() {
//    return 0;
//  }
//
//  @Override
//  public boolean isSupportedType(Type genericType) {
//    return false;
//  }
//
//  @Override
//  public void collectQueryStats(ObjectGraphNode objectGraphNode, long loadedBeanCount, long timeMicros) {
//
//  }
//
//  @Override
//  public void loadMany(BeanCollection<?> collection, boolean onlyIds) {
//
//  }
//
//  @Override
//  public void loadBean(EntityBeanIntercept ebi) {
//
//  }
//
//  @Override
//  public void shutdown(boolean shutdownDataSource, boolean deregisterDriver) {
//
//  }
//
//  @Override
//  public AutoTune getAutoTune() {
//    return null;
//  }
//
//  @Override
//  public String getName() {
//    return name;
//  }
//
//  @Override
//  public ExpressionFactory getExpressionFactory() {
//    return null;
//  }
//
//  @Override
//  public MetaInfoManager getMetaInfoManager() {
//    return null;
//  }
//
//  @Override
//  public BeanState getBeanState(Object bean) {
//    return null;
//  }
//
//  @Override
//  public Object getBeanId(Object bean) {
//    return null;
//  }
//
//  @Override
//  public Map<String, ValuePair> diff(Object a, Object b) {
//    return null;
//  }
//
//  @Override
//  public <T> T createEntityBean(Class<T> type) {
//    return null;
//  }

}
