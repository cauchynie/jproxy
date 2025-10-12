package org.pierce.httpproxy.handler;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.pierce.MessageBridge;
import org.pierce.UtilTools;
import org.pierce.bridge.ByteBufMessageBridge;
import org.pierce.bridge.HttpMessageBridge;
import org.pierce.entity.ProtocolInfo;
import org.pierce.list.entity.TryConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpHeaderNames.*;

public class HttpProxyServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(HttpProxyServerHandler.class);

    String lastMethod = null;

    MessageBridge messageBridge = null;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("{}:{}", UtilTools.formatChannelInfo(ctx), msg);

        if (messageBridge != null) {
            messageBridge.bridge(ctx.channel(), msg);
            return;
        }
        if (msg instanceof HttpRequest request) {

            String uri = request.uri();
            log.info(uri);
            ProtocolInfo protocolInfo = UtilTools.parseProtocolInfo(uri);


            String targetAddress = protocolInfo.getHostAddress();
            int targetPort = protocolInfo.getPort();

            ctx.channel().attr(ByteBufMessageBridge.TARGET_ADDRESS).set(targetAddress);
            ctx.channel().attr(ByteBufMessageBridge.TARGET_PORT).set(targetPort);


            if ("CONNECT".equalsIgnoreCase(request.method().name())) {
                messageBridge = new ByteBufMessageBridge();
                messageBridge.bridge(ctx.channel(), new TryConnect()).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {

                        if (future.isSuccess()) {
                            log.info("future.isSuccess()");
                            FullHttpResponse response = new DefaultFullHttpResponse(
                                    HttpVersion.HTTP_1_1,
                                    HttpResponseStatus.OK
                            );
                            response.headers()
                                    .set(CONTENT_TYPE, "text/plain; charset=UTF-8")
                                    .set(CONTENT_LENGTH, response.content().readableBytes())
                                    .set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                            ctx.channel().writeAndFlush(response).addListener(new GenericFutureListener<Future<? super Void>>() {
                                @Override
                                public void operationComplete(Future<? super Void> future) throws Exception {
                                    if (ctx.channel().isActive()) {
                                        if (ctx.channel().pipeline().get(HttpServerCodec.class) != null) {
                                            ctx.channel().pipeline().remove(HttpServerCodec.class);
                                        }
                                    }
                                }
                            });
                        } else {
                            log.warn("!future.isSuccess()", future.cause());
                            FullHttpResponse response = new DefaultFullHttpResponse(
                                    HttpVersion.HTTP_1_1,
                                    HttpResponseStatus.REQUEST_TIMEOUT
                            );
                            response.headers()
                                    .set(CONTENT_TYPE, "text/plain; charset=UTF-8")
                                    .set(CONTENT_LENGTH, response.content().readableBytes())
                                    .set(CONNECTION, HttpHeaderValues.CLOSE);
                            ctx.channel().writeAndFlush(response).addListener(new GenericFutureListener<Future<? super Void>>() {
                                @Override
                                public void operationComplete(Future<? super Void> future) throws Exception {
                                    if (ctx.channel().isActive()) {
                                        if (ctx.channel().pipeline().get(HttpServerCodec.class) != null) {
                                            ctx.channel().pipeline().remove(HttpServerCodec.class);
                                        }
                                    }
                                }
                            });
                        }
                    }
                });
            } else {
                messageBridge = new HttpMessageBridge();
                messageBridge.bridge(ctx.channel(), msg);
            }

        }


    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.info("{} ", UtilTools.formatChannelInfo(ctx), cause);
        ctx.close();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        log.info("{} ", UtilTools.formatChannelInfo(ctx));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("{} ", UtilTools.formatChannelInfo(ctx));
    }


}