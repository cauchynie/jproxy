package org.pierce;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.apache.ibatis.session.SqlSession;
import org.pierce.httpproxy.HttpProxyServer;
import org.pierce.imp.DefaultSelector;
import org.pierce.impl.DefaultConnectionTypeCheck;
import org.pierce.list.Directive;
import org.pierce.list.imp.DataBaseNameListCheck;
import org.pierce.list.imp.FixedReturnConnectListCheck;
import org.pierce.list.imp.GFWNameListCheck;
import org.pierce.list.imp.TextNameListCheck;
import org.pierce.manage.ManageServer;
import org.pierce.mybatis.entity.HostName2Address;
import org.pierce.mybatis.mapper.HostName2AddressMapper;
import org.pierce.socks.SocksServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalServer {

    private final static LocalServer instance = new LocalServer();

    private static final Logger log = LoggerFactory.getLogger(LocalServer.class);

    //private NameListCheck nameListCheck;

    public ConnectionTypeCheck connectionTypeCheck;


    private final Selector<String> stringSelector = new DefaultSelector<>();

    // private final Selector<HostName2Address> hostName2AddressDefaultSelector = new DefaultSelector<>();

    private LocalServer() {

    }

    public static LocalServer getInstance() {
        return instance;
    }

    public void initialize() {
        Jproxy.getInstance().initialize(getClass(), true);
        connectionTypeCheck = new DefaultConnectionTypeCheck();
    }


    public synchronized void updateHostAddress(String label, String domain, String address) {
        try (SqlSession sqlSession = DataBase.getSqlSessionFactory().openSession()) {
            HostName2AddressMapper mapper = sqlSession.getMapper(HostName2AddressMapper.class);
            HostName2Address hostName2Address = mapper.selectOneByHostNameAndAddress(domain, address);
            if (hostName2Address == null) {
                hostName2Address = new HostName2Address();
                hostName2Address.setHostName(domain);
                hostName2Address.setAddress(address);
                hostName2Address.setLabel(label);
                hostName2Address.setPriority(0);
                hostName2Address.setStatus("00");
                hostName2Address.setUpdateTime(UtilTools.currentTime());
                mapper.insert(hostName2Address);

            }
            sqlSession.commit();
        }
    }


    public Selector<String> getStringSelector() {
        return stringSelector;
    }

    public static void main(String[] args) {

        System.setProperty("javax.net.debug", "ssl:handshake");
        LocalServer.getInstance().initialize();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                eventLoopGroup.shutdownGracefully();
                log.info("eventLoopGroup.shutdownGracefully()");
            }
        });


        JproxyServer manageServer = new ManageServer();
        manageServer.start(eventLoopGroup);


        //+0.FixedReturnConnectListCheck
        JproxyServer socksServer = new SocksServer("default-name-check",new FixedReturnConnectListCheck());
        socksServer.start(eventLoopGroup);

        //+1.DataBaseNameListCheck
        socksServer = new SocksServer("db-name-check", DataBaseNameListCheck.getInstance());
        socksServer.start(eventLoopGroup);

        //+2.TextNameListCheck
        socksServer = new SocksServer("text-name-check", new TextNameListCheck() {
            {
                loadByInputStream();
            }

        });
        socksServer.start(eventLoopGroup);

        //+3.GFWNameListCheck
        socksServer = new SocksServer("gfw-name-check", GFWNameListCheck.getInstance());
        socksServer.start(eventLoopGroup);


        socksServer = new SocksServer("local-link-out",new FixedReturnConnectListCheck(Directive.DIRECT_CONNECT));
        socksServer.start(eventLoopGroup);

        ////////// http proxy
        //+0.FixedReturnConnectListCheck
        JproxyServer httpProxyServer = new HttpProxyServer("default-name-check",new FixedReturnConnectListCheck());
        httpProxyServer.start(eventLoopGroup);


        //+1.DataBaseNameListCheck
        httpProxyServer = new HttpProxyServer("db-name-check", DataBaseNameListCheck.getInstance());
        httpProxyServer.start(eventLoopGroup);

        //+2.TextNameListCheck
        httpProxyServer = new HttpProxyServer("text-name-check", new TextNameListCheck() {
            {
                loadByInputStream();
            }

        });
        httpProxyServer.start(eventLoopGroup);

        //+3.GFWNameListCheck
        httpProxyServer = new HttpProxyServer("gfw-name-check", GFWNameListCheck.getInstance());
        httpProxyServer.start(eventLoopGroup);

        httpProxyServer = new HttpProxyServer("local-link-out",new FixedReturnConnectListCheck(Directive.DIRECT_CONNECT));
        httpProxyServer.start(eventLoopGroup);
    }
}
