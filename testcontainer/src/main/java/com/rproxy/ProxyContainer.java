package com.rproxy;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import reactor.core.Disposable;

/**
 * Test container that starts remote proxy server and proxies traffic to localhost
 * 
 * @author azukovskij
 *
 */
public class ProxyContainer extends GenericContainer<ProxyContainer> {
    
    private static final String DOCKER_IMAGE_NAME = "azukovskij/rproxy:1.0.0";
    
    private static final int PROXY_POT = 7878;
    private int maxConnections = 32;
    private List<Integer> proxiedPorts;
    private List<Disposable> proxies;

    public ProxyContainer() {
        super(DOCKER_IMAGE_NAME);
    }
    
    /**
     * Ports to listen on proxy server and to proxy to localhost
     * 
     * @param proxiedPorts port number list
     */
    public void setProxiedPorts(List<Integer> proxiedPorts) {
        this.proxiedPorts = proxiedPorts;
    }
    
    /**
     * Maximum number of connections to allow
     * 
     * @param maxConnections
     */
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }
    
    @Override
    protected void configure() {
        var port = String.valueOf(7878);
        setPortBindings(Stream.concat(
                getPortBindings().stream(), 
                Stream.of(port))
            .collect(Collectors.toList()));
        addEnv("HTTP_PORT", port);
        addEnv("MAX_CONNECTIONS", String.valueOf(maxConnections));
        setWaitStrategy(new LogMessageWaitStrategy()
            .withRegEx(".*Started proxy on port.*")
            .withStartupTimeout(Duration.ofSeconds(30)));
    }
    
    
    @Override
    protected void doStart() {
        super.doStart();
        
        var porxyAddress = new InetSocketAddress(getHost(), getMappedPort(PROXY_POT));
        this.proxies = proxiedPorts.stream()
            .map(p -> new RSocketProxyClient(porxyAddress, new InetSocketAddress(p), maxConnections))
            .collect(Collectors.toUnmodifiableList());
    }
    
    @Override
    public void stop() {
        if (proxies != null) {
            proxies.forEach(Disposable::dispose);
        }
        super.stop();
    }
    
}
