package com.rproxy;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpServer;
import reactor.pool.Pool;
import reactor.pool.PoolBuilder;

/**
 * Server part of the proxy, listens for ports requested by the {@link RSocketProxyClient}
 * and proxies incoming requests on those ports back to the client
 * 
 * @author azukovskij
 *
 */
public class RSocketProxyServer implements Disposable {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<Integer, NettyServer> servers = new ConcurrentHashMap<Integer, NettyServer>();
    private final int maxConnections;
    private final Disposable disposable;

    RSocketProxyServer(int port, int maxConnections) {
        this.maxConnections = maxConnections;
        this.disposable = RSocketServer.create(
                SocketAcceptor.forRequestChannel(inbound -> 
                    Flux.create(sink -> 
                        sink.onDispose(Flux.from(inbound)
                            .subscribe(request -> receive(request, sink))))))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .bind(TcpServerTransport.create(port))
            .doOnSubscribe(s -> logger.info("Started proxy on port {}", port))
            .subscribe(Closeable::onClose);
    }
    
    private void receive(Payload request, FluxSink<Payload> inbound) {
        if (!request.hasMetadata()) {
            return;
        }
        var route = Route.deserialize(request.metadata());
        servers.computeIfAbsent(route.getPort(), k -> new NettyServer(k, inbound))
            .send(route, request.data());
    }

    @Override
    public void dispose() {
        disposable.dispose();
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
        private final Disposable disposable;
        
        NettyServer(int port, FluxSink<Payload> inbound) {
            var counter = new AtomicInteger();
            this.routes = PoolBuilder.<Route>from(Mono.fromCallable(() -> 
                    new Route(port, counter.incrementAndGet() % maxConnections)))
                .sizeBetween(0, maxConnections)
                .buildPool();
            this.disposable = TcpServer.create().port(port)   
                .doOnBind(c -> logger.info("Listening for port {}", port))
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
                .subscribe();
        }
        
        /**
         * sends response to the server (back from the client)
         */
        void send(Route route, ByteBuf data) {
            Optional.ofNullable(channels.get(route))
                .ifPresent(sink -> sink.next(data));
        }
        
        @Override
        public void dispose() {
            disposable.dispose();
        }
        
    }
    
}
