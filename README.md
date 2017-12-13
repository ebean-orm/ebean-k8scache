# ebean-k8scache
L2 Cache support running in Kubernetes


## Steps 

1. Add ebean-k8scache as a dependency
2. Configure the kubernetes service name and port

### Example configuration

```yaml
ebean:
  cluster:
    active: true
    serviceName: my-service
    port: 9911
```

The above means that ebean-k8scache will discover all the pods for
`my-service` and join them as a cluster using port 9911.

All the pods will run a L2 Cache and cache invalidation messages will
be propagated to all the pods in the cluster as needed.

## Configuration

As an example of configuration via code:

```java

ContainerConfig container = new ContainerConfig();
container.setActive(true);
container.setPort(9911);
container.setServiceName("my-service");

// On the ServerConfig of the default Server
// set the ContainerConfig
ServerConfig serverConfig = new ServerConfig();
serverConfig.setContainerConfig(container);


// When the container of the default starts it will discover
// the other pods and join them as a cluster 
EbeanServer defaultServer = EbeanServerFactory.create(serverConfig);

``` 

## Runtime membership checking

Periodically the membership of the cluster is checked. By default this is done every
minute and back off to every 5 minutes. 
