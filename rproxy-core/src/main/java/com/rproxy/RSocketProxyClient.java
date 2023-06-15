package com.rproxy;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.core.RSocketClient;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.pool.Pool;
import reactor.pool.PoolBuilder;
import reactor.util.retry.Retry;

/**
 * Client part of the proxy, notifies server to start listening to requested ports and 
 * forwards requests received by the server into local host
 * 
 * @author azukovskij
 *
 */
public class RSocketProxyClient implements Disposable {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Set<Subscription> subscriptions = ConcurrentHashMap.newKeySet();
    private final Pool<Connection> pool;

    private final InetSocketAddress proxyAddress;
    private final InetSocketAddress localAddress;
    
    /**
     * Creates client and connects to {@link RSocketProxyServer} 
     * 
     * @param proxyAddress address of remote {@link RSocketProxyServer} to proxy from
     * @param localAddress local address to to proxy to
     * @param maxConnections outbound (local) connection pool maximum size
     */
    public RSocketProxyClient(InetSocketAddress proxyAddress, InetSocketAddress localAddress, int maxConnections) {
        this.proxyAddress = proxyAddress;
        this.localAddress = localAddress;
        this.pool = PoolBuilder.<Connection>from(Mono.defer(() -> TcpClient.create()
                .host(localAddress.getHostName())
                .port(localAddress.getPort()).connect()))
            .destroyHandler(c -> Mono.fromRunnable(c::dispose))
            .evictionPredicate((c,ref) -> c.isDisposed())
            .sizeBetween(1, maxConnections)
            .buildPool();
    }

    /**
     * Connects to server
     */
    public Mono<Void> connect() {
        var route = new Route(localAddress.getPort(), null);
        var responses = Sinks.many().unicast().<Payload>onBackpressureBuffer();
        return RSocketClient.from(RSocketConnector.create()
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .reconnect(Retry.backoff(100, Duration.ofMillis(10)))
                .connect(TcpClientTransport.create(proxyAddress)))
            .requestChannel(responses.asFlux())
            .groupBy(req -> Route.deserialize(req.metadata()), 1)
            .flatMap(requests -> pool.acquire()
                .flatMap(ref -> {
                    var connection = ref.poolable();
                    return Mono.when(
                        connection.outbound().send(requests
                            .doOnNext(p -> logger.info("forwarding request to {}", localAddress))
                            .map(Payload::data)).then(),
                        connection.inbound().receive().retain()
                            .map(data -> DefaultPayload.create(data, requests.key().serialize()))
                            .doOnNext(responses::tryEmitNext)
                        )
                        .doOnError(e -> logger.error("connection to {} failed", localAddress, e))
                        .onErrorComplete()
                        .then(Mono.defer(ref::release));
                }))
            .doOnSubscribe(s -> responses.tryEmitNext(DefaultPayload.create(Unpooled.EMPTY_BUFFER, route.serialize())))
            .doOnSubscribe(subscriptions::add)
            .then();
    }

    @Override
    public void dispose() {
        subscriptions.forEach(Subscription::cancel);
        pool.dispose();
    }
    
}
