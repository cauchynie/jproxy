package org.pierce.bridge;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.*;
import org.pierce.Jproxy;
import org.pierce.MessageBridge;
import org.pierce.UtilTools;
import org.pierce.handler.DebugHandler;
import org.pierce.list.entity.MessageWrap;
import org.pierce.list.entity.TryConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class ByteBufMessageBridge implements MessageBridge {


    public static final AttributeKey<Channel> LINK_IN = AttributeKey.valueOf("LINK_IN");

    public static final AttributeKey<String> TARGET_ADDRESS = AttributeKey.valueOf("TARGET_ADDRESS");

    public static final AttributeKey<Integer> TARGET_PORT = AttributeKey.valueOf("TARGET_PORT");


    private static final Logger log = LoggerFactory.getLogger(ByteBufMessageBridge.class);

    Channel lastLinkOut = null;

    final Queue<MessageWrap> queue = new LinkedList<>();

    int status = 0;

    Throwable cause = null;

    @Override
    public ChannelPromise bridge(Channel linkIn, Object message) {
        ChannelPromise channelPromise = linkIn.newPromise();
        if (message instanceof TryConnect) {
            status = 1;
            String targetAddress = linkIn.attr(TARGET_ADDRESS).get();
            int targetPort = linkIn.attr(TARGET_PORT).get();
            bridge0(targetAddress, targetPort, linkIn, channelPromise);


            return channelPromise;
        }
        synchronized (queue) {
            if (cause != null) {
                channelPromise.tryFailure(cause);
            } else if (lastLinkOut == null) {
                queue.add(new MessageWrap(channelPromise, message));
            } else {
                lastLinkOut.writeAndFlush(message).addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if (future.isSuccess()) {
                            channelPromise.trySuccess();
                        } else {
                            channelPromise.tryFailure(future.cause());
                        }
                    }
                });
            }
        }


        return channelPromise;
    }

    public void bridge0(String targetAddress, int targetPort, Channel linkIn, ChannelPromise channelPromise) {
        Future<Channel> channelFuture;
        channelFuture = acquireA(targetAddress, targetPort);

        channelFuture.addListener((GenericFutureListener<Future<Channel>>) future -> {
            if (future.isSuccess()) {
                Channel channel = future.getNow();
                channel.attr(LINK_IN).set(linkIn);
                channelPromise.trySuccess();

                synchronized (queue) {
                    while (!queue.isEmpty()) {
                        MessageWrap messageWrap = queue.remove();
                        channel.writeAndFlush(messageWrap.message()).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (future.isSuccess()) {
                                    messageWrap.promise().trySuccess();
                                } else {
                                    messageWrap.promise().tryFailure(future.cause());
                                }
                            }
                        });
                    }
                }

                lastLinkOut = channel;
                return;
            } else {
                channelPromise.tryFailure(future.cause());
                cause = future.cause();
                synchronized (queue) {

                    while (!queue.isEmpty()) {
                        MessageWrap messageWrap = queue.remove();
                        messageWrap.promise().tryFailure(future.cause());
                    }
                }
            }
        });

    }

    public Promise<Channel> acquireA(String target, int port) {

        EventExecutor executor = ImmediateEventExecutor.INSTANCE;
        Promise<Channel> channelPromise = executor.newPromise();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(Jproxy.getEventLoopGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {


                channel.pipeline().addLast(new DebugHandler("byte-link-out"));
                channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        log.info("{}:{}", UtilTools.formatChannelInfo(ctx), msg);

                        {
                            StringBuilder sb = new StringBuilder();
                            ChannelPipeline pipeline = ctx.pipeline();
                            sb.append("===== Pipeline Structure =====\n");
                            for (Map.Entry<String, ChannelHandler> entry : pipeline) {
                                String name = entry.getKey();
                                ChannelHandler handler = entry.getValue();
                                sb.append(String.format("Handler [%s] -> %s\n", name, handler.getClass().getSimpleName()));
                            }
                            log.info(sb.toString());
                        }

                        log.info("{}:{}", UtilTools.formatChannelInfo(ctx), msg);
                        Channel linkIn = ctx.channel().attr(LINK_IN).get();
                        if (linkIn != null) {
                            if (!linkIn.isActive()) {
                                //级联关闭
                                ctx.channel().close();
                                return;
                            }
                            linkIn.attr(LINK_OUT).set(ctx.channel());
                            linkIn.writeAndFlush(msg).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        if (msg instanceof ByteBuf byteBuf) {
                                            byteBuf.release();
                                        }
                                    }
                                }
                            });
                            ;
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        log.error("Throwable", cause);
                        if (ctx.channel().isActive()) {
                            ctx.channel().close();
                        }
                        channelPromise.tryFailure(cause);
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                        log.info("channelInactive");

                        Channel linkIn = ctx.channel().attr(LINK_IN).get();
                        //级联关闭
                        if (linkIn != null) {
                            linkIn.close();
                        }

                    }

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        channelPromise.trySuccess(ctx.channel());
                    }
                });
            }

        });
        log.info("connect to {}:{} start", target, port);
        ChannelFuture cf = bootstrap.connect(target, port);
        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    log.info("connect to {}:{} future.isSuccess", target, port);
                    return;
                } else {
                    channelPromise.tryFailure(future.cause());
                    log.info("connect to {}:{} future.fail", target, port);
                }
            }
        });
        return channelPromise;
    }


}
