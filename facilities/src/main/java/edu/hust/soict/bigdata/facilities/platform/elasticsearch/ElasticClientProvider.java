package edu.hust.soict.bigdata.facilities.platform.elasticsearch;

import com.google.common.net.HostAndPort;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by sondn on 2020/03/10
 */
public class ElasticClientProvider {

    private static Map<String, RestHighLevelClient> clients = new TreeMap<>();

    public static RestHighLevelClient getDefault(ElasticConfig config) {
        return getOrCreate("default", config);
    }

    public static synchronized RestHighLevelClient getOrCreate(String name, ElasticConfig config) {
        synchronized (ElasticClientProvider.class) {
            return clients.computeIfAbsent(name, k -> initElasticClient(config));
        }
    }

    private static RestHighLevelClient initElasticClient(ElasticConfig config) {
        Collection<HostAndPort> hostAndPorts = config.getHosts();
        HttpHost[] httpHosts = hostAndPorts.stream().map(m -> new HttpHost(m.getHost(), m.getPort())).toArray(HttpHost[]::new);

        return new RestHighLevelClient(
                RestClient.builder(httpHosts));
    }
}
