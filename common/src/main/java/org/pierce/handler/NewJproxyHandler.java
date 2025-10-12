package org.pierce.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameEncoder;
import io.netty.handler.proxy.ProxyHandler;
import org.pierce.JproxyProperties;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

import static io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13.MAGIC_GUID;

public class NewJproxyHandler extends ProxyHandler {

    String expectedServerKey;

    public NewJproxyHandler(SocketAddress proxyAddress) {
        super(proxyAddress);
    }

    @Override
    public String protocol() {
        return "websocket";
    }

    @Override
    public String authScheme() {
        return "basic";
    }

    HttpClientCodecWrapper httpClientCodec = new HttpClientCodecWrapper();

    @Override
    protected void addCodec(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().pipeline().addBefore(ctx.name(), "http-codec", httpClientCodec);
        ctx.channel().pipeline().addBefore(ctx.name(), "http-aggregator", new HttpObjectAggregator(1024));
    }


    @Override
    protected void removeEncoder(ChannelHandlerContext ctx) throws Exception {
        httpClientCodec.codec.removeOutboundHandler();

        ctx.pipeline().addBefore(ctx.name(), "ws-decoder", new WebSocket13FrameDecoder(false, true, 65536, false));
        ctx.pipeline().addBefore(ctx.name(), "ws-encoder", new WebSocket13FrameEncoder(true));
    }

    @Override
    protected void removeDecoder(ChannelHandlerContext ctx) throws Exception {
        httpClientCodec.codec.removeInboundHandler();
        ctx.pipeline().remove("http-aggregator");
    }


    @Override
    protected Object newInitialMessage(ChannelHandlerContext ctx) throws Exception {

        InetSocketAddress raddr = destinationAddress();

        SecureRandom secureRandom = new SecureRandom();
        byte[] bytes = new byte[16];
        secureRandom.nextBytes(bytes);
        String clientKey = new String(Base64.getEncoder().encode(bytes));

        expectedServerKey = Base64.getEncoder().encodeToString(
                MessageDigest.getInstance("SHA-1")
                        .digest((clientKey + MAGIC_GUID).getBytes(StandardCharsets.UTF_8))
        );
        

        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, JproxyProperties.getProperty("local-server.link-out.websocket-path"), Unpooled.EMPTY_BUFFER);

        request.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET)
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE)
                .set(HttpHeaderNames.SEC_WEBSOCKET_KEY, clientKey)
                .set(HttpHeaderNames.ORIGIN, JproxyProperties.evaluate("https://${local-server.link-out.address}"))
                .set(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "13")
                .set("TARGET_ADDRESS", raddr.getHostString())
                .set("TARGET_PORT", raddr.getPort())
                .set("WORK_TYPE", "01");
        //log.info(request.toString());
        return request;
    }

    @Override
    protected boolean handleResponse(ChannelHandlerContext ctx, Object response) throws Exception {
        if (response instanceof FullHttpResponse fullHttpResponse) {
            if (fullHttpResponse.status().code() != 101) {
                throw new RuntimeException("((FullHttpResponse) msg).status().code()!=200");
            }
            String accept = fullHttpResponse.headers().get(HttpHeaderNames.SEC_WEBSOCKET_ACCEPT);
            if (!expectedServerKey.equals(accept)) {
                //log.info("{} != {}", expectedServerKey, accept);
                throw new RuntimeException("!expectedServerKey.equals(accept)");
            }
            return true;
        }
        throw new RuntimeException("!response instanceof FullHttpResponse fullHttpResponse");
    }

    private static final class HttpClientCodecWrapper implements ChannelInboundHandler, ChannelOutboundHandler {
        final HttpClientCodec codec = new HttpClientCodec();

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            codec.handlerAdded(ctx);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            codec.handlerRemoved(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            codec.exceptionCaught(ctx, cause);
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            codec.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            codec.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            codec.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            codec.channelInactive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            codec.channelRead(ctx, msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            codec.channelReadComplete(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            codec.userEventTriggered(ctx, evt);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            codec.channelWritabilityChanged(ctx);
        }

        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress,
                         ChannelPromise promise) throws Exception {
            codec.bind(ctx, localAddress, promise);
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                            ChannelPromise promise) throws Exception {
            codec.connect(ctx, remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            codec.disconnect(ctx, promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            codec.close(ctx, promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            codec.deregister(ctx, promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) throws Exception {
            codec.read(ctx);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            codec.write(ctx, msg, promise);
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            codec.flush(ctx);
        }
    }
}
