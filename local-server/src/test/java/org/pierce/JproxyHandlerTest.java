package org.pierce;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.pierce.handler.DebugHandler;
import org.pierce.handler.NewJproxyHandler;
import org.pierce.handler.TlsClientHandlerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class JproxyHandlerTest {

    private static final Logger log = LoggerFactory.getLogger(JproxyHandlerTest.class);

    public static final EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    log.info("eventLoopGroup.shutdownGracefully();");
                    eventLoopGroup.shutdownGracefully();
                } catch (Exception e) {
                    log.info("eventLoopGroup.shutdownGracefully();", e);
                }


            }
        });


        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                if (JproxyProperties.booleanVal("tls-debug")) {
                    ch.pipeline().addLast(new DebugHandler("tls-link-out"));
                }
                if (JproxyProperties.booleanVal("local-server.link-out.tls")) {
                    ch.pipeline().addLast(TlsClientHandlerBuilder.getInstance().build(ch));
                }
                ch.pipeline().addLast(new DebugHandler("link-out"));
                ch.pipeline().addLast(new NewJproxyHandler(new InetSocketAddress(JproxyProperties.getProperty("local-server.link-out.address"), Integer.parseInt(JproxyProperties.getProperty("local-server.remote-websocket-link-out.port")))));
                ch.pipeline().addLast(new ChannelDuplexHandler() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        if (msg instanceof BinaryWebSocketFrame binaryWebSocketFrame) {
                            ctx.fireChannelRead(binaryWebSocketFrame.content().copy());
                        }
                    }

                    @Override
                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                        if (msg instanceof ByteBuf byteBuf) {
                            ctx.write(new BinaryWebSocketFrame(byteBuf), promise);
                            return;
                        }
                        //promise.tryFailure(new RuntimeException("msg instanceof ByteBuf byteBuf"));
                    }
                });
                ch.pipeline().addLast(new HttpClientCodec());
                ch.pipeline().addLast(new HttpObjectAggregator(65536));

                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        log.info("channelRead:{}", msg);
                    }

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        log.info("channelActive");
                        FullHttpRequest fullHttpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
                        fullHttpRequest.headers().set("Host", "192.168.31.129");
                        fullHttpRequest.headers().set("User-Agent", "curl/8.14.1");
                        fullHttpRequest.headers().set("Accept", "*/*");
                        ctx.writeAndFlush(fullHttpRequest);
                    }
                });
            }

        });
        log.info("connect to {}:{} start", "192.168.31.129", 80);
        ChannelFuture cf = bootstrap.connect("192.168.31.129", 80);
        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    log.info("connect to {}:{} future.isSuccess", "192.168.31.129", 80);
                    return;
                } else {
                    log.info("connect to {}:{} future.fail", "192.168.31.129", 80);
                }
            }
        });
    }
}