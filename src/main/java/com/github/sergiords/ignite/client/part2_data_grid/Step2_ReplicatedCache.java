package com.github.sergiords.ignite.client.part2_data_grid;

import com.github.sergiords.ignite.client.ClientStep;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

@SuppressWarnings({"unused", "ConstantConditions", "Duplicates"})
public class Step2_ReplicatedCache implements ClientStep {

    private static final String CACHE_NAME = "my-replicated-cache";

    private final Ignite ignite;

    private final IgniteCache<String, String> cache;

    public Step2_ReplicatedCache(Ignite ignite) {
        this.ignite = ignite;

        /*
         * TODO:
         * - create a CacheConfiguration named "my-replicated-cache"
         * - set the cache mode to REPLICATED in this configuration
         * - create and return a cache using this configuration
         */
        this.cache = null;
    }

    @Override
    public void run() {

        CacheObserver cacheObserver = new CacheObserver(ignite, cache);

        cacheObserver.run();

    }

    @Override
    public void close() {
        ignite.destroyCache(CACHE_NAME);
    }

}
