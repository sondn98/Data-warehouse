package edu.hust.soict.bigdata.collector.worker;

import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.hystrix.monitor.MetricsServer;
import edu.hust.soict.bigdata.facilities.common.util.Reflects;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class ScrapingListener {

    private static final Logger logger = LoggerFactory.getLogger(ScrapingListener.class);

    public static void start(Properties props) throws Exception {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        String port = props.getProperty(CollectorConst.LISTENER_SERVER_PORT, "6969");
        final String serverAddress = props.getProperty(CollectorConst.LISTENER_SERVER_ADDRESS, "localhost");
        final String url = "http://" + serverAddress + ":" + port;
        URI uri = URI.create(url);

        Class[] apis = Reflects.getClass(props.getCollection(CollectorConst.LISTENER_SERVER_API_CLASSES));
        ResourceConfig resourceConfig = new ResourceConfig(apis);

        logger.info("Jersey on Jetty container started. Try out " + url);

        MetricsServer metricsServer = new MetricsServer(props);
        metricsServer.start();

        Server server = JettyHttpContainerFactory.createServer(uri, resourceConfig, false);

        for(Connector y : server.getConnectors()) {
            for(ConnectionFactory x  : y.getConnectionFactories()) {
                if(x instanceof HttpConnectionFactory) {
                    ((HttpConnectionFactory)x).getHttpConfiguration().setSendServerVersion(false);
                }
            }
        }

        server.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Metrics Server...");
            metricsServer.stop();
        }));
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties()
                .addResource("collector.properties");
        logger.info(props.toString());

        start(props);
        logger.info("Server is running ...");
    }
}
