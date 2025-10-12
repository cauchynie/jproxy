package org.pierce.httpproxy.handler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import org.pierce.JproxyProperties;
import org.pierce.handler.DebugHandler;
import org.pierce.handler.TlsServerHandlerBuilder;
import org.pierce.list.NameListCheck;
import org.pierce.session.SessionAttributes;


public final class HttpProxyServerInitializer extends ChannelInitializer<SocketChannel> {


    NameListCheck nameListCheck;


    public HttpProxyServerInitializer(NameListCheck nameListCheck) {
        this.nameListCheck = nameListCheck;
    }


    @Override
    public void initChannel(SocketChannel ch) throws Exception {


        if (nameListCheck != null) {
            ch.attr(SessionAttributes.NAME_LIST_CHECK).set(nameListCheck);
        }


        // 添加 TLS 处理器到管道
        if (JproxyProperties.booleanVal("local-server.link-in.tls")) {
            if (JproxyProperties.booleanVal("tls-debug")) {
                ch.pipeline().addLast(new DebugHandler("tls-link-in"));
            }
            ch.pipeline().addLast(TlsServerHandlerBuilder.getInstance().build(ch));
        }
        ch.pipeline().addLast(new DebugHandler("link in"));

        ch.pipeline().addLast(new HttpServerCodec());
        ch.pipeline().addLast(new HttpProxyServerHandler());
    }
}
