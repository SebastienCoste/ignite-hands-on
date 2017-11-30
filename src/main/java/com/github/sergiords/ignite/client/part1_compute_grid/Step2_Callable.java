package com.github.sergiords.ignite.client.part1_compute_grid;

import com.github.sergiords.ignite.server.ServerApp;
import org.apache.ignite.Ignite;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Step2_Callable {

    private final Ignite ignite;

    public Step2_Callable(Ignite ignite) {
        this.ignite = ignite;
    }

    public String getResultFromOneNode() {

        /*
         * TODO:
         * - use ignite.compute().call(...) to return a computation result from one (random) server node
         * - return ServerApp.getName() in computation
         */

        String serverName = ignite.compute().call(() -> ServerApp.getName());
        ignite.compute().run(() -> ServerApp.print(serverName));
        return serverName;
    }

    public Collection<String> getResultFromAllNodes() {

        /*
         * TODO:
         * - use ignite.compute().broadcast(...) to return computation results from all server nodes
         * - return ServerApp.getName() in computation
         */

        Collection<String> allNames = ignite.compute().broadcast(() -> ServerApp.getName());
        allNames.stream()
            .forEach(n -> ignite.compute().run(() -> ServerApp.print(n)));
        return allNames;
    }

    public Collection<String> getResultFromTwoNodes() {

        /*
         * TODO:
         * - use ignite.compute().call(...) to return computation results from two server nodes
         * - return ServerApp.getName() in first computation
         * - return ServerApp.getInfo() in second computation
         */
        String name = ignite.compute().call(() -> ServerApp.getName());
        String info = ignite.compute().call(() -> ServerApp.getInfo());
        List<String> data = Arrays.asList(name, info);
        data.stream()
            .forEach(d -> ignite.compute().run(() -> ServerApp.print(d)));
        return data;
    }

}
