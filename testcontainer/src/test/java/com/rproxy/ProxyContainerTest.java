/* Copyright © 2022 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.rproxy;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;

public class ProxyContainerTest {
    
    private static final int PORT = firstAvailablePort();
    
    @RegisterExtension
    public static WireMockExtension wireMock = WireMockExtension.extensionOptions()
        .options(WireMockConfiguration.options()
            .port(PORT))
        .build();
    
    @Container
    public static ProxyContainer container = new ProxyContainer()
        .addPortBindings(String.valueOf(PORT))
        .withProxiedPorts(List.of(PORT));
    
    private final HttpClient httpClient = HttpClient.newBuilder()
        .version(Version.HTTP_2)
        .build();
    
    @BeforeAll
    public static void init() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable());
        container.start();
    }
    
    @Test
    public void shouldProxy() throws IOException, InterruptedException {
        wireMock.stubFor(get(urlPathEqualTo("/"))
            .willReturn(ok()
                .withBody("Test Response")));
        
        var response = Awaitility.await()
            .atMost(Duration.ofSeconds(3))
            .until(() -> request(proxyUrl()), resp -> resp.statusCode() == 200);
        assertThat(response.body(), equalTo("Test Response"));
    }


    private URI proxyUrl() {
        return URI.create("http://" +  container.getHost() + ":" 
            + container.getMappedPort(wireMock.getPort()));
    }

    private HttpResponse<String> request(URI uri) throws IOException, InterruptedException {
        return httpClient.send(HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build(), BodyHandlers.ofString());
    }
    
    private static int firstAvailablePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            fail("Port is not available");
            return -1;
        }
    }

}
