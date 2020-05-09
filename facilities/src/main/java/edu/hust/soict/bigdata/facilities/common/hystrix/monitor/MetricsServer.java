package edu.hust.soict.bigdata.facilities.common.hystrix.monitor;

import com.netflix.hystrix.contrib.metrics.eventstream.HystrixMetricsStreamServlet;
import com.soundcloud.prometheus.hystrix.HystrixPrometheusMetricsPublisher;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class MetricsServer {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServer.class);
    private String host;
    private int port;

    private Server server;

    public MetricsServer(Properties props) {
        this.host = props.getProperty(Const.HYSTRIX_METRICS_SERVER_HOST, "localhost");
        this.port = props.getIntProperty(Const.HYSTRIX_METRICS_SERVER_PORT, 7998);
    }

    public void start() {
        server = new Server(new InetSocketAddress(host, port));

        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HystrixMetricsStreamServlet.class, "/hystrix.stream");
        handler.addServletWithMapping(new ServletHolder(new MetricsServlet()), "/metrics");

        server.setHandler(handler);

        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        HystrixPrometheusMetricsPublisher.register("data-collection",registry);

        try {
            server.start();
            logger.info("Hystrix metrics server has started at http://" + host + ":" + port);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void join() {
        try {
            server.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
