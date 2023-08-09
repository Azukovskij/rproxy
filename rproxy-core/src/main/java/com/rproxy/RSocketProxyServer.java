package com.rproxy;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.pool.Pool;
import reactor.pool.PoolBuilder;
import reactor.util.retry.Retry;

/**
 * Server part of the proxy, listens for ports requested by the {@link RSocketProxyClient}
 * and proxies incoming requests on those ports back to the client
 * 
 * @author azukovskij
 *
 */
public class RSocketProxyServer implements Disposable {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Set<Subscription> subscriptions = ConcurrentHashMap.newKeySet();
    private final Map<Integer, NettyServer> servers = new ConcurrentHashMap<Integer, NettyServer>();
    private final int port;
    private final int maxConnections;
    

    RSocketProxyServer(int port, int maxConnections) {
        this.maxConnections = maxConnections;
        this.port = port;
    }
    
    /**
     * Starts the server 
     */
    public Mono<CloseableChannel> start() {
        return RSocketServer.create(
            SocketAcceptor.forRequestChannel(inbound -> 
                        Flux.create(sink -> 
                            sink.onDispose(Flux.from(inbound)
                                .doOnCancel(() -> disconnect(sink))
                                .subscribe(request -> receive(request, sink), sink::error, sink::complete)))))
                .resume(new Resume()
                        .retry(Retry.backoff(100, Duration.ofMillis(10))))
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .bind(TcpServerTransport.create(port))
                .doOnSubscribe(s -> logger.info("Started proxy on port {}", port))
                .doOnSubscribe(subscriptions::add)
                .doOnNext(Closeable::onClose);
    }

    private void receive(Payload request, FluxSink<Payload> inbound) {
        if (!request.hasMetadata()) {
            return;
        }
        var route = Route.deserialize(request.metadata());
        servers.computeIfAbsent(route.getPort(), k -> createServer(k, inbound))
            .send(route, request.data());
    }
    
    private void disconnect(FluxSink<Payload> sink) {
        servers.values().removeIf(s -> s.disposeSink(sink));
        sink.complete();
    }

    // @VisibleForTesting
    NettyServer createServer(int port, FluxSink<Payload> inbound) {
        return new NettyServer(port, port, inbound);
    }

    @Override
    public void dispose() {
        subscriptions.forEach(Subscription::cancel);
        servers.forEach((k,v) -> v.dispose());
    }
    

    /**
     * Serves TCP/IP incoming connections on proxy server side
     * 
     * @author azukovskij
     *
     */
    class NettyServer implements Disposable {
        
        private final Map<Route, FluxSink<ByteBuf>> channels = new ConcurrentHashMap<>();
        private final Pool<Route> routes;
        private final AtomicReference<DisposableServer> server = new AtomicReference<>();
        private final FluxSink<Payload> inbound;
        
        NettyServer(int serverPort, int clientPort, FluxSink<Payload> inbound) {
            this.inbound = inbound;
            var counter = new AtomicInteger();
            this.routes = PoolBuilder.<Route>from(Mono.fromCallable(() -> 
                    new Route(clientPort, counter.incrementAndGet() % maxConnections)))
                .sizeBetween(0, maxConnections)
                .buildPool();
            TcpServer.create().port(serverPort)   
                .doOnBind(c -> logger.info("Listening for port {}", serverPort))
                .handle((in, out) -> routes.acquire()
                    .flatMap(ref -> out.send(Flux.create(outbound -> {
                            var route = ref.poolable(); 
                            outbound.onDispose(in.receive().retain()
                                .map(data -> DefaultPayload.create(data, route.serialize()))
                                .subscribe(inbound::next, inbound::error));
                            channels.put(route, outbound);
                        }))
                        .then(Mono.defer(ref::release)).then()))
                .bind()
                .subscribe(server::set);
        }
        
        /**
         * sends response to the server (back from the client)
         */
        void send(Route route, ByteBuf data) {
            Optional.ofNullable(channels.get(route))
                .ifPresent(sink -> sink.next(data));
        }
        
        /**
         * @return true if server channel was disposed 
         */
        boolean disposeSink(FluxSink<Payload> inbound) {
            boolean listensOn = this.inbound == inbound;
            if (listensOn) {
                dispose();
            }
            return listensOn;
        }
        
        @Override
        public void dispose() {
            Optional.ofNullable(server.get())
                .ifPresent(DisposableServer::dispose);
        }
        
    }
    
}
