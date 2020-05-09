package edu.hust.soict.bigdata.facilities.platform.elasticsearch;

import com.google.common.net.HostAndPort;
import edu.hust.soict.bigdata.facilities.common.config.Properties;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by kumin on 13/02/2017.
 */
public class ElasticConfig {


    private Collection<HostAndPort> hosts = new ArrayList<>();
    private String clusterName;

    public ElasticConfig(Properties p) {
        Collection<String> addresses = p.getCollection("elastic.hosts");
        addresses.forEach(addr -> hosts.add(HostAndPort.fromString(addr)));
        this.clusterName = p.getProperty("elastic.cluster.name");
    }

    public Collection<HostAndPort> getHosts() {
        return hosts;
    }

    public String getClusterName() {
        return clusterName;
    }
}
