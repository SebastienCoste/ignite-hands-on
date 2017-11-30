package com.github.sergiords.ignite.client.part2_data_grid;

import com.github.sergiords.ignite.data.Data;
import com.github.sergiords.ignite.data.Team;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.TextQuery;

import javax.cache.Cache;
import java.util.List;
import java.util.stream.Collectors;

public class Step1_CacheQuery {

    private final IgniteCache<Integer, Team> cache;

    public Step1_CacheQuery(Ignite ignite) {

        /*
         * TODO:
         * - create a CacheConfiguration named "my-cache"
         * - use ignite.getOrCreateCache(...) to create a cache with this configuration
         */
        this.cache = ignite.getOrCreateCache("my-cache");
    }

    public void populateCache() {

        List<Team> teams = Data.teams();
        teams.stream()
            .forEach(t -> this.cache.put(t.getId(), t));
        /*
         * TODO:
         * - put all teams in the cache using team.getId() as cache key
         */

    }

    public Team findByKey(Integer id) {

        /*
         * TODO:
         * - find team in cache by id
         */
        return this.cache.get(id);
    }

    public String findByKeyAndProcess(Integer id) {

        // Entry Processor

        /*
         * TODO:
         * - create a CacheEntryProcessor returning team name in uppercase
         * - use this processor and cache.invoke(...) to return processed team name for given id
         */
        return cache.invoke(id,(i,t) -> i.getValue().getName().toUpperCase());
    }

    public List<Team> findByScanQuery(String nameSearch) {

        // Scan query

        /*
         * TODO:
         * - create a ScanQuery finding teams whose name ends with given nameSearch
         * - use cache.query(...) to return teams from cache matching this query
         */


        ScanQuery<Integer, Team> sq = new ScanQuery<>((k, v) -> v.getName().endsWith(nameSearch));

        return this.cache.query(sq)
            .getAll()
            .stream()
            .map(Cache.Entry::getValue)
            .collect(Collectors.toList());
    }

    public List<Team> findByTextQuery(String nameSearch) {

        // Text Query

        /*
         * TODO:
         * - add @QueryTextField annotation to Team.name field to allow Lucene-based Text search on name
         * - create a TextQuery finding teams matching 'nameSearch' (see tests to find what search criteria looks like)
         * - use cache.query(...) to return teams from cache matching this query
         * TIP:
         * - define indexed types (cache key and cache value types) in cache configuration (see configuration.setIndexedTypes)
         */

        TextQuery<Integer, Team> tq = new TextQuery<Integer, Team>(String.class, nameSearch);
        return this.cache.query(tq)
            .getAll()
            .stream()
            .map(Cache.Entry::getValue)
            .collect(Collectors.toList());
    }

    public List<Team> findBySqlQuery(String nameSearch) {

        // SQL Query

        /*
         * TODO:
         * - add @QuerySqlField annotation to Team.name field to allow SQL query name searches
         * - create a SqlQuery finding teams where name is like 'nameSearch' (see tests to find what search criteria looks like)
         * - query example: 'select * from team where name like ?'
         * - use cache.query(...) to return teams from cache matching this query
         * TIP:
         * - define indexed types (cache key and cache value types) in cache configuration (see configuration.setIndexedTypes)
         */

        return null;
    }

}
