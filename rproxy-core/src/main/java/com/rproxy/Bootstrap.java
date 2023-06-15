package com.rproxy;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.TypeHandler;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Main class for starting client/server from command line
 * 
 * @author azukovskij
 *
 */
public class Bootstrap {

    private static int DEFAULT_PROXY_PORT = 7878;
    private static int DEFAULT_CONNECTIONS = 32;
    
    public static void main(String[] args) throws InterruptedException {
        var formatter = new HelpFormatter();

        if (args.length == 0 || !("server".equals(args[0]) || "client".equals(args[0]))) {
            formatter.printHelp(ServerOptions.USAGE, ServerOptions.OPTIONS);
            formatter.printHelp(ClientOptions.USAGE, ClientOptions.OPTIONS);
            System.exit(1);
        }

        var latch = new CountDownLatch(1);
        switch (args[0]) {
            case "server":
                try {
                    var opts = new ServerOptions(args);
                    var server = new RSocketProxyServer(opts.getProxyPort(), opts.getMaxConnections());
                    subscribe(server.start(), latch);
                    registerShutdownHook(server, latch::countDown);
                    latch.await();
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                    formatter.printHelp(ServerOptions.USAGE, ServerOptions.OPTIONS);
                }
                break;
            case "client":
                try {
                    var opts = new ClientOptions(args);
                    var proxyAddress = opts.getProxyAddress();
                    var maxConnections = opts.getMaxConnections();
                    var clients = opts.getLocalAdresses().stream()
                        .map(addr -> new RSocketProxyClient(proxyAddress, addr, maxConnections))
                        .collect(Collectors.toList());
                    subscribe(Flux.fromIterable(clients)
                        .flatMap(RSocketProxyClient::connect)
                        .then(), latch);
                    registerShutdownHook(clients.toArray(Disposable[]::new));
                    registerShutdownHook(latch::countDown);
                    latch.await();
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                    formatter.printHelp(ClientOptions.USAGE, ClientOptions.OPTIONS);
                }
                break;
        }

    }

    private static void subscribe(Mono<?> start, CountDownLatch latch) {
        start.subscribe(v -> {}, e -> latch.countDown(), latch::countDown);
    }
    
    private static void registerShutdownHook(Disposable... disposables) {
        Runtime.getRuntime().addShutdownHook(new Thread() { 
            @Override
            public void run() {
                Arrays.stream(disposables).forEach(Disposable::dispose);
            }
        });
    }
    
    /**
     * {@link RSocketProxyServer} command line startup options
     * 
     * @author azukovskij
     *
     */
    static class ServerOptions {

        private static final String USAGE = "server [options]";
        private static final Options OPTIONS = new Options()
            .addOption(Option.builder("p")
                .longOpt("port")
                .type(Number.class).hasArg()
                .desc("proxy port").build())
            .addOption(Option.builder("c")
                .type(Number.class).hasArg()
                .longOpt("maxconnections")
                .desc("max inbound connections")
                .build());
        
        private final CommandLine commandLine;
        
        public ServerOptions(String[] args) throws ParseException {
            this.commandLine = new DefaultParser().parse(OPTIONS, 
                Arrays.copyOfRange(args, 1, args.length));
        }
        
        
        int getProxyPort() throws ParseException {
            return Optional.ofNullable((Number)commandLine.getParsedOptionValue("p"))
                .map(Number::intValue)
                .orElse(DEFAULT_PROXY_PORT);
        }
        
        int getMaxConnections() throws ParseException {
            return Optional.ofNullable((Number)commandLine.getParsedOptionValue("c"))
                .map(Number::intValue)
                .orElse(DEFAULT_CONNECTIONS);
        }
        
    }
    
    /**
     * {@link RSocketProxyClient} command line startup options
     * 
     * @author azukovskij
     *
     */
    static class ClientOptions {

        private static final String USAGE = "client [options]";
        private static final Options OPTIONS = new Options()
            .addOption(Option.builder("t")
                .longOpt("proxy")
                .type(URL.class).hasArg().required()
                .desc("proxy address").build())
            .addOption(Option.builder("p")
                .longOpt("ports")
                .type(Number.class).hasArgs().required()
                .valueSeparator(',')
                .desc("proxied ports").build())
            .addOption(Option.builder("c")
                .longOpt("maxconnections")
                .type(Number.class).hasArg()
                .desc("max outbound connections")
                .build());
        
        private CommandLine commandLine;
        
        public ClientOptions(String[] args) throws ParseException {
            this.commandLine = new DefaultParser().parse(OPTIONS, 
                Arrays.copyOfRange(args, 1, args.length));
        }
        
        InetSocketAddress getProxyAddress() throws ParseException {
            var url = (URL)commandLine.getParsedOptionValue("t");
            if (!url.getPath().isBlank()) {
                throw new ParseException("proxy url should not contain path");
            }
            return new InetSocketAddress(url.getHost(), url.getPort());
        }
        
        List<InetSocketAddress> getLocalAdresses() throws ParseException {
            var result = new ArrayList<InetSocketAddress>();
            for(String port : commandLine.getOptionValues("p")) {
                result.add(new InetSocketAddress("localhost", TypeHandler.createNumber(port).intValue()));
            }
            return result;
        }
        
        int getMaxConnections() throws ParseException {
            return Optional.ofNullable((Number)commandLine.getParsedOptionValue("c"))
                .map(Number::intValue)
                .orElse(DEFAULT_CONNECTIONS);
        }
        
    }
    
    
}
