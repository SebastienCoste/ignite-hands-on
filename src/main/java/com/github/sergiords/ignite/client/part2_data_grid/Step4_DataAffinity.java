package com.github.sergiords.ignite.client.part2_data_grid;

import com.github.sergiords.ignite.data.Team;
import com.github.sergiords.ignite.data.User;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKey;

import java.util.List;
import java.util.Optional;

public class Step4_DataAffinity {

    private static final String CACHE_NAME = "my-data-affinity-cache";

    private final Ignite ignite;

    private final IgniteCache<AffinityKey<Team>, List<User>> cache;

    public Step4_DataAffinity(Ignite ignite) {

        this.ignite = ignite;

        /*
         * TODO:
         * - create a cache configuration with name "my-data-affinity-cache"
         * - notice type for keys is AffinityKey<Team> not just Team
         * - use this configuration to create a cache
         */

        this.cache = null;
    }

    public AffinityKey<Team> affinityKey(Team team) {

        /*
         * TODO:
         * - return a new affinity key using team's country as the affinity key discriminator
         * - use new AffinityKey(...)
         */
        return null;
    }

    public void populateCache() {

        /*
         * TODO:
         * - populate cache (Affinity<Team> => List<User>)
         * - put all teams from the same country in the same node using affinityKey method defined above
         * - use Data.teams() to find teams
         * - use Data.users(team) to find users for a team
         */
    }

    public Optional<User> findTopCommitterFromCountry(String country) {

        /*
         * TODO:
         * - return top committer for the given country
         *
         * - keep in mind that all teams from a country are hosted in same node
         * - you can safely use ignite.compute().affinityCall(...) to make an affinity search in proper node (use country as affinity key)
         * - on this node, use cache.localEntries() to get all locally stored teams
         * TIP:
         * - cache.localEntries() returns an Iterable... that's annoying
         * - use StreamSupport.stream(myIterator.spliterator(), false) to get a plain-old stream
         */
        return null;
    }

}
