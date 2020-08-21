package edu.hust.soict.bigdata.facilities.platform.elasticsearch;

import com.google.common.net.HostAndPort;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.*;

/**
 * @author sondn
 * @since 2020/03/10
 */
public class ElasticClientProvider extends ObjectPool<RestHighLevelClient> {

    private static ElasticClientProvider eProvider;

    public static ElasticClientProvider getInstance(){
        if(eProvider == null)
            eProvider = new ElasticClientProvider();
        return eProvider;
    }

    @Override
    @SuppressWarnings("UnstableApiUsage")
    protected RestHighLevelClient create() {
        logger.info("Creating elasticsearch RestHighLevelClient...");

        List<HostAndPort> hosts = new LinkedList<>();
        Collection<String> addresses = Config.getCollection(Const.ELASTIC_HOST);
        addresses.forEach(addr -> hosts.add(HostAndPort.fromString(addr)));
        HttpHost[] httpHosts = hosts.stream().map(m -> new HttpHost(m.getHost(), m.getPort())).toArray(HttpHost[]::new);

        return new RestHighLevelClient(RestClient.builder(httpHosts));
    }

}
