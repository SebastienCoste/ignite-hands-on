package com.github.sergiords.ignite.client.part4_service;

import com.github.sergiords.ignite.client.ClientStep;
import org.apache.ignite.Ignite;

@SuppressWarnings({"unused", "ConstantConditions"})
public class Step4_AffinityService implements ClientStep {

    private static final String CACHE_NAME = "my-affinity-service-cache";

    private static final String SERVICE_NAME = "/my-affinity-service/key42";

    private final Ignite ignite;

    public Step4_AffinityService(Ignite ignite) {

        this.ignite = ignite;

        /*
         * TODO:
         * - create a partitioned cache named "my-affinity-service-cache" just like in Step1_PartitionedCache
         */

        /*
         * TODO:
         * - deploy a service named "/my-affinity-service/key42"
         * - this service should be deployed on node where "Key42" is stored for the cache "my-affinity-service-cache"
         * - see: IgniteServices#deployKeyAffinitySingleton
         */
    }

    @Override
    public void run() {

        Integer[] numbers = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        String key = "Key42";

        String nodeWithKey = findNodeWithKey(key);

        Integer sumFromService = executeOnNodeWithService(numbers, key);

        System.out.printf("=== Affinity Service ===%n");
        System.out.printf("Node for Key42 is: %s%n", nodeWithKey);
        System.out.printf("Sum for numbers is: %s%n", sumFromService);
    }

    private String findNodeWithKey(String key) {

        /*
         * TODO:
         * - return the NodeId where given key is stored
         * - use IgniteCompute#affinityCall
         * - retrieve NodeId using System.getProperty("node.id")
         */
        return null;
    }

    private Integer executeOnNodeWithService(Integer[] numbers, String key) {

        /*
         * TODO:
         * - now we can use IgniteServices#service to get locally deployed service
         */
        return null;
    }

    @Override
    public void close() {
        ignite.services().cancel(SERVICE_NAME);
        ignite.destroyCache(CACHE_NAME);
    }

}
