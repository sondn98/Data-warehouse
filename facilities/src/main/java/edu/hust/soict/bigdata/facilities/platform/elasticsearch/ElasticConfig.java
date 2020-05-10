package edu.hust.soict.bigdata.facilities.platform.elasticsearch;

import com.google.common.net.HostAndPort;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author sondn
 * @since 13/02/2020
 */
@SuppressWarnings("UnstableApiUsage")
public class ElasticConfig {


    private Collection<HostAndPort> hosts = new ArrayList<>();
    private String clusterName;

    public ElasticConfig(Properties props) {
        Collection<String> addresses = props.getCollection(Const.ELASTIC_HOST);
        addresses.forEach(addr -> hosts.add(HostAndPort.fromString(addr)));
        this.clusterName = props.getProperty(Const.ELASTIC_CLUSTER_NAME);
    }

    public Collection<HostAndPort> getHosts() {
        return hosts;
    }

    public String getClusterName() {
        return clusterName;
    }
}
