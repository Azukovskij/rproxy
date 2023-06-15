package com.rproxy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Represents connection route from socket on {@link RSocketProxyServer} to socket on {@link RSocketProxyClient}
 * 
 * @author azukovskij
 *
 */
public class Route implements Serializable {
    
    private static final long serialVersionUID = -4809185921199610434L;
    
    private final int port;
    private final Integer channelId;
    
    public Route(int port, Integer channelId) {
        this.port = port;
        this.channelId = channelId;
    }
    
    /**
     * @return server port
     */
    public int getPort() {
        return port;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(channelId, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Route)) {
            return false;
        }
        Route other = (Route) obj;
        return Objects.equals(channelId, other.channelId) && port == other.port;
    }

    public ByteBuf serialize() {
        try(var baos = new ByteArrayOutputStream(); var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(this);
            return Unpooled.wrappedBuffer(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    public static Route deserialize(ByteBuf metadata) {
        var bytes = new byte[metadata.readableBytes()];
        metadata.getBytes(0, bytes);
        try(var bais = new ByteArrayInputStream(bytes); var ois = new ObjectInputStream(bais)){
            return (Route) ois.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
    
}
