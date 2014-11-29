package org.devtools.river;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class GitlabRiver extends AbstractRiverComponent implements River {
    private final Client client;

    private GitlabRiverLogic riverLogic;

    @Inject
    public GitlabRiver(final RiverName riverName, final RiverSettings settings,
            final Client client) {
        super(riverName, settings);
        this.client = client;

        logger.info("CREATE GitlabRiver");

        // TODO Your code..

    }

    @Override
    public void start() {
        logger.info("START GitlabRiver");

        riverLogic = new GitlabRiverLogic();
        new Thread(riverLogic).start();
    }

    @Override
    public void close() {
        logger.info("CLOSE GitlabRiver");

        // TODO Your code..
    }

    private class GitlabRiverLogic implements Runnable {

        @Override
        public void run() {
            logger.info("START GitlabRiverLogic: " + client.toString());

            // TODO Your code..
        }
    }
}
