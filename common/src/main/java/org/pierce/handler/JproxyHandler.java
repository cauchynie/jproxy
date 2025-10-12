package org.pierce.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import org.pierce.JproxyProperties;
import org.pierce.UtilTools;
import org.pierce.list.entity.MessageWrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedList;
import java.util.Queue;

import static io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13.MAGIC_GUID;

public class JproxyHandler extends ChannelDuplexHandler {

    // WebSocket握手使用的GUID
    //private static final String MAGIC_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    InetSocketAddress proxyAddress;

    InetSocketAddress targetAddress;


    final static Logger log = LoggerFactory.getLogger(JproxyHandler.class);

    final Queue<MessageWrap> writeQueue = new LinkedList<>();

    final Queue<MessageWrap> readQueue = new LinkedList<>();

    public JproxyHandler(InetSocketAddress proxyAddress) {
        this.proxyAddress = proxyAddress;
    }

    boolean complete = false;

    //String clientKey;

    String expectedServerKey;

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        targetAddress = (InetSocketAddress) remoteAddress;
        super.connect(ctx, proxyAddress, localAddress, promise);

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        SecureRandom secureRandom = new SecureRandom();
        byte[] bytes = new byte[16];
        secureRandom.nextBytes(bytes);
        String clientKey = new String(Base64.getEncoder().encode(bytes));

        expectedServerKey = Base64.getEncoder().encodeToString(
                MessageDigest.getInstance("SHA-1")
                        .digest((clientKey + MAGIC_GUID).getBytes(StandardCharsets.UTF_8))
        );
        ctx.channel().pipeline().addBefore(ctx.name(), "http-codec", new HttpClientCodec());
        ctx.channel().pipeline().addBefore(ctx.name(), "http-aggregator", new HttpObjectAggregator(1024));

        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, JproxyProperties.getProperty("local-server.link-out.websocket-path"), Unpooled.EMPTY_BUFFER);

        request.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET)
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE)
                .set(HttpHeaderNames.SEC_WEBSOCKET_KEY, clientKey)
                .set(HttpHeaderNames.ORIGIN, JproxyProperties.evaluate("https://${local-server.link-out.address}"))
                .set(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "13")
                .set("TARGET_ADDRESS", targetAddress.getHostString())
                .set("TARGET_PORT", targetAddress.getPort())
                .set("WORK_TYPE", "01");
        //log.info(request.toString());
        ctx.writeAndFlush(request);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.info("{}-{}:{}", complete, UtilTools.formatChannelInfo(ctx), msg);
        if (!complete) {
            if (msg instanceof FullHttpResponse fullHttpResponse) {
                if (fullHttpResponse.status().code() != 101) {
                    ctx.fireExceptionCaught(new RuntimeException("((FullHttpResponse) msg).status().code()!=200"));
                    return;
                }
                //log.info("{} {}", UtilTools.formatChannelInfo(ctx), msg);
                String accept = fullHttpResponse.headers().get(HttpHeaderNames.SEC_WEBSOCKET_ACCEPT);
                if (!expectedServerKey.equals(accept)) {
                    //log.info("{} != {}", expectedServerKey, accept);
                    ctx.fireExceptionCaught(new RuntimeException("!expectedServerKey.equals(accept)"));
                }
                ctx.pipeline().remove("http-codec");
                ctx.pipeline().remove("http-aggregator");
                ctx.pipeline().addBefore(ctx.name(), "ws-decoder", new WebSocket13FrameDecoder(false, true, 65536, false));
                ctx.pipeline().addBefore(ctx.name(), "ws-encoder", new WebSocket13FrameEncoder(true));
                while (!writeQueue.isEmpty()) {
                    MessageWrap messageWrap = writeQueue.remove();
                    if (messageWrap.message() instanceof ByteBuf) {
                        BinaryWebSocketFrame binaryWebSocketFrame = new BinaryWebSocketFrame((ByteBuf) messageWrap.message());
                        ctx.writeAndFlush(binaryWebSocketFrame);
                    } else {
                        ctx.writeAndFlush(messageWrap.message());
                    }

                }
                complete = true;
                ctx.fireChannelActive();


            } else {
                ctx.fireExceptionCaught(new RuntimeException(msg + "!msg instanceof FullHttpResponse"));
            }
            return;
        }

        if (msg instanceof FullHttpResponse response) {
            throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus=" + response.status() +
                            ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

//        if (!complete) {
//            //log.info("{}:{}", UtilTools.formatChannelInfo(ctx), msg.getClass());
//            writeQueue.add(new MessageWrap(promise, msg));
//            return;
//        }
        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof BinaryWebSocketFrame binaryWebSocketFrame) {
            try {
                ctx.fireChannelRead(binaryWebSocketFrame.content().copy());
            } finally {
                binaryWebSocketFrame.release();
            }
        } else if (frame instanceof PongWebSocketFrame) {
            log.info("WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            log.info("WebSocket Client received closing");
            ctx.close();
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        log.info("{}-{}:{}", complete, UtilTools.formatChannelInfo(ctx), msg);
        if (!complete) {
            //log.info("{}:{}", UtilTools.formatChannelInfo(ctx), msg.getClass());
            writeQueue.add(new MessageWrap(promise, msg));
            return;
        }
        if (msg instanceof ByteBuf) {

            BinaryWebSocketFrame binaryWebSocketFrame = new BinaryWebSocketFrame((ByteBuf) msg);
            ctx.write(binaryWebSocketFrame).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        binaryWebSocketFrame.release();
                    }

                }
            });
            return;
        }

        super.write(ctx, msg, promise);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("WebSocket Client disconnected!");
        while (!writeQueue.isEmpty()) {
            MessageWrap messageWrap = writeQueue.remove();
            messageWrap.promise().tryFailure(new Throwable("channelInactive"));
            if (messageWrap.message() instanceof ByteBuf byteBuf) {
                byteBuf.release();
            } else if (messageWrap.message() instanceof BinaryWebSocketFrame binaryWebSocketFrame) {
                binaryWebSocketFrame.release();
            }

        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.info("exception", cause);
        ctx.close();
    }

    //    public static String generateWebSocketAccept(String key) {
    //        try {
    //            // 拼接客户端密钥和GUID
    //            String input = key + MAGIC_GUID;
    //
    //            // 获取SHA-1实例
    //            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
    //
    //            // 计算哈希值
    //            byte[] hash = sha1.digest(input.getBytes(StandardCharsets.UTF_8));
    //
    //            // Base64编码
    //            return Base64.getEncoder().encodeToString(hash);
    //        } catch (NoSuchAlgorithmException e) {
    //            throw new RuntimeException("SHA-1算法不可用", e);
    //        }
    //    }
}


