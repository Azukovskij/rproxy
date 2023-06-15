package com.rproxy;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Test container that starts remote proxy server and proxies traffic to localhost
 * 
 * @author azukovskij
 *
 */
public class ProxyContainer extends GenericContainer<ProxyContainer> {
    
    private static final String DOCKER_IMAGE_NAME = "azukovskij/rproxy:1.0.0";
    
    private static final int PROXY_POT = 7878;
    private final List<Disposable> disposables = new ArrayList<>();
    private int maxConnections = 32;
    private List<Integer> proxiedPorts;

    public ProxyContainer() {
        super(DOCKER_IMAGE_NAME);
    }

    public ProxyContainer(String dockerImageName) {
        super(dockerImageName);
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
     * Ports to listen on proxy server and to proxy to localhost
     * 
     * @param proxiedPorts port number list
     * @return container
     */
    public ProxyContainer withProxiedPorts(List<Integer> proxiedPorts) {
        this.proxiedPorts = proxiedPorts;
        return this;
    }
    
    /**
     * Maximum number of connections to allow
     * 
     * @param maxConnections number of connections
     */
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Maximum number of connections to allow
     * 
     * @param maxConnections number of connections
     * @return container
     */
    public ProxyContainer withMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }

    /**
     * Adds port binding
     * 
     * @see #setPortBindings(List)
     * 
     * @param binding port binding
     * @return container
     */
    public ProxyContainer addPortBindings(String binding) {
        setPortBindings(Stream.concat(
            getPortBindings().stream(), 
            Stream.of(binding))
        .collect(Collectors.toList()));
        return this;
    }
    
    @Override
    protected void configure() {
        var port = String.valueOf(7878);
        addPortBindings(port);
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
        var proxies = proxiedPorts.stream()
            .map(p -> new RSocketProxyClient(porxyAddress, new InetSocketAddress(p), maxConnections))
            .collect(Collectors.toUnmodifiableList());
        this.disposables.addAll(proxies);
        this.disposables.add(Flux.fromIterable(proxies)
            .flatMap(RSocketProxyClient::connect)
            .subscribe());
    }
    
    @Override
    public void stop() {
        disposables.forEach(Disposable::dispose);
        super.stop();
    }
    
}
