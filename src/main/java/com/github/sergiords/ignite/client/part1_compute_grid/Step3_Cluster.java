package com.github.sergiords.ignite.client.part1_compute_grid;

import com.github.sergiords.ignite.server.ServerApp;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;

import java.util.Collection;

public class Step3_Cluster {

    private final Ignite ignite;

    public Step3_Cluster(Ignite ignite) {
        this.ignite = ignite;
    }

    public ClusterGroup customClusterGroup() {

        /*
         * TODO:
         * - get a cluster group composed of nodes with attribute "node.id" = "Server1" or "node.id" = "Server2"
         * - use ignite.cluster().forPredicate(...) to find appropriate nodes
         * FYI:
         * - ClusterNode.attribute() method exposes system property of remote server node
         */
        return ignite.cluster().forPredicate(p ->
            p.attribute("node.id") != null && (
            p.attribute("node.id").equals("Server1")
            || p.attribute("node.id").equals("Server2")
        )
        );
    }

    public Collection<String> runInCustomClusterGroup() {

        /*
         * TODO:
         * - run a computation returning Server.getName() for all nodes in previous custom cluster group
         * - use ignite.compute(...).broadcast(...)
         */
        ClusterGroup clusterGroup = customClusterGroup();
        return ignite.compute(clusterGroup).broadcast(ServerApp::getName);
    }

}
