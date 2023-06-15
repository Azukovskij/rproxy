/* Copyright © 2022 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.rproxy;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;

import io.rsocket.Payload;
import io.rsocket.transport.netty.server.CloseableChannel;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;

public class RSocketProxyTest {

    @RegisterExtension
    public static WireMockExtension wireMock = new WireMockExtension();
    
    private final HttpClient httpClient = HttpClient.newBuilder()
        .version(Version.HTTP_2)
        .build();
    
    private int rsocketPort;
    
    private List<Disposable> disposeables = new ArrayList<>();
    
    private Map<Integer, URI> serverUris = new ConcurrentHashMap<>();
    
    @BeforeEach
    public void init() {
        stubFor(get(urlPathEqualTo("/"))
            .willReturn(ok()
                .withBody("Test Response")));
        this.rsocketPort = firstAvailablePort();
    }
    
    @AfterEach
    public void cleanUp() throws Exception {
        for(var disposeable : disposeables) {
            disposeable.dispose();
        }
    }

    @Test
    public void shouldProxy() throws IOException, InterruptedException {
        startServer();
        connectClient();
        
        assertThat(request(proxyUrl()).body(), equalTo("Test Response"));
    }

    @Test
    public void shouldProxyMultipleClients() throws IOException, InterruptedException {
        var server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        server.stubFor(get(urlPathEqualTo("/"))
            .willReturn(ok()
                .withBody("Test Response 2")));
        server.start();
        disposeables.add(server::stop);
        
        startServer();
        connectClient();
        connectClient(server.port());
        
        Awaitility.await()
            .atMost(Duration.ofSeconds(3))
            .until(() -> serverUris.containsKey(server.port()));
        
        assertThat(request(proxyUrl()).body(), equalTo("Test Response"));
        assertThat(request(serverUris.get(server.port())).body(), equalTo("Test Response 2"));
    }

    @Test
    public void shouldWaitForServer() throws IOException, InterruptedException {
        connectClient();
        Thread.sleep(100L);
        startServer();
        
        assertThat(request(proxyUrl()).body(), equalTo("Test Response"));
    }

    @Test
    public void shouldAllowServerRestart() throws Exception {
        var d = startServer();
        connectClient();
        
        assertThat(request(proxyUrl()).body(), equalTo("Test Response"));
        
        d.dispose();
        startServer();
        
        assertThat(request(proxyUrl()).body(), equalTo("Test Response"));
    }

    @Test
    public void shouldAllowClientRestart() throws Exception {
        startServer();
        var d = connectClient();
        
        assertThat(request(proxyUrl()).body(), equalTo("Test Response"));
        
        d.dispose();
        connectClient();
        
        assertThat(request(proxyUrl()).body(), equalTo("Test Response"));
    }
    
    @Test
    public void shouldAwaitTargetRestart() throws Exception {
        var port = firstAvailablePort();
        var server = new WireMockServer(WireMockConfiguration.options().port(port));
        server.stubFor(get(urlPathEqualTo("/"))
            .willReturn(ok()
                .withBody("Test Response 3")));
        server.start();
        disposeables.add(server::stop);
        
        startServer();
        connectClient(server.port());

        Awaitility.await()
            .atMost(Duration.ofSeconds(3))
            .until(() -> serverUris.containsKey(server.port()));
        
        assertThat(request(serverUris.get(server.port())).body(), equalTo("Test Response 3"));
        
        server.stop();
        Thread.sleep(100L);
        server.start();
        
        assertThat(request(serverUris.get(server.port())).body(), equalTo("Test Response 3"));
    }

    private URI proxyUrl() {
        return Awaitility.await()
            .atMost(Duration.ofSeconds(3))
            .until(() -> serverUris.get(wireMock.getPort()), Objects::nonNull);
    }
    
    private Disposable startServer() {
        var server = new TestRSocketProxyServer();
        var channel = new AtomicReference<CloseableChannel>();
        var disposable = server.start().subscribe(channel::set);
        var closable = (Disposable) () -> {
            Optional.ofNullable(channel.get()).ifPresent(CloseableChannel::dispose);
            disposable.dispose();
            server.dispose();
        };
        disposeables.add(closable);
        return closable;
    }
    
    private Disposable connectClient() {
        return connectClient(wireMock.getPort()); 
    }

    private Disposable connectClient(int port) {
        var client = new RSocketProxyClient(new InetSocketAddress(rsocketPort), 
            new InetSocketAddress(port), 1);
        var disposable = client.connect().subscribe();
        var closable = (Disposable) () -> {
            disposable.dispose();
            client.dispose();
        };
        disposeables.add(closable);
        return closable;
    }
    
    private static int firstAvailablePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            fail("Port is not available");
            return -1;
        }
    }

    private HttpResponse<String> request(URI uri) throws IOException, InterruptedException {
        return httpClient.send(HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build(), BodyHandlers.ofString());
    }
    
    class TestRSocketProxyServer extends RSocketProxyServer {

        TestRSocketProxyServer() {
            super(rsocketPort, 4);
        }

        @Override
        NettyServer createServer(int port, FluxSink<Payload> inbound) {
            var serverPort = firstAvailablePort();
            serverUris.put(port, URI.create("http://localhost:" + serverPort));
            return new NettyServer(serverPort, port, inbound);
        }
        
    }

}
