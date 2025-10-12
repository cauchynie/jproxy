package org.pierce.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import org.pierce.JproxyProperties;
import org.pierce.UtilTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A {@link ChannelHandler} that logs all events using a logging framework.
 * By default, all events are logged at <tt>DEBUG</tt> level and full hex dumps are recorded for ByteBufs.
 */
@Sharable
public class DebugHandler extends ChannelDuplexHandler {

    private static final Logger log = LoggerFactory.getLogger(DebugHandler.class);

    String title = "";

    public DebugHandler() {
    }

    public DebugHandler(String title) {
        this.title = title;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logObj(ctx, "入站数据", msg);
        if (msg instanceof ByteBuf) {
            logBytes(ctx, "入站数据", (ByteBuf) msg);
        }
        super.channelRead(ctx, msg); // 传递给后续处理器
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        logObj(ctx, "出站数据", msg);
        if (msg instanceof ByteBuf) {
            // 复制ByteBuf并记录日志，不影响原数据
            logBytes(ctx, "出站数据", (ByteBuf) msg);
        }
        super.write(ctx, msg, promise); // 传递给后续处理器
    }

    private void logObj(ChannelHandlerContext ctx, String direction, Object object) {
        log.info("{} {} {} {},{}", UtilTools.formatChannelInfo(ctx), title, direction, object.getClass(), String.valueOf(object));
        if (!JproxyProperties.booleanVal("debug")) {
            return;
        }
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

    }

    private void logBytes(ChannelHandlerContext ctx, String direction, ByteBuf buf) {
        if (!JproxyProperties.booleanVal("debug")) {
            return;
        }


        // 创建副本避免影响原缓冲区的读写索引
        ByteBuf copy = buf.copy();
        try {

            //byte[] bytes = new byte[copy.readableBytes()];
            //copy.readBytes(bytes);
            log.info("{} {} {} \n{}", UtilTools.formatChannelInfo(ctx), title, direction, ByteBufUtil.prettyHexDump(copy));
            /*log.info(String.format("%s %s [%d bytes]: %s\n%s %s [%d bytes]: %s",
                    title,
                    direction,
                    bytes.length,
                    bytesToHex(bytes), title,
                    direction,
                    bytes.length,
                    byteToString(bytes)));*/
/*            log.info(String.format("%s %s [%d bytes]: %s",
                    title,
                    direction,
                    bytes.length,
                    byteToString(bytes)));*/

        } finally {
            copy.release(); // 释放副本资源
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02X ", b));
        }
        return hex.toString().trim();
    }

    private String byteToString(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            if (b > 32 && b < 126) {
                hex.append(String.format("  %c", b));
            } else {
                hex.append("  .");
            }

        }
        return hex.toString().trim();
    }
}
