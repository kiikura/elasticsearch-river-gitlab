package org.devtools.river;

import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class GitlabRiver extends AbstractRiverComponent implements River {
    private final Client client;

    private GitlabRiverLogic riverLogic;

	private volatile Thread riverThread;
	
	private volatile boolean closed;

    @Inject
    public GitlabRiver(final RiverName riverName, final RiverSettings settings,
            final Client client) {
        super(riverName, settings);
        this.client = client;

        logger.info("CREATE GitlabRiver");

        // TODO Your code..
        //Requests.indexRequest().source()
    }

    @Override
    public void start() {
        logger.info("START GitlabRiver");
        Map<String,Object> gitlab = (Map<String,Object>)this.settings.settings().get("gitlab");
        
        riverLogic = new GitlabRiverLogic();
        this.riverThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "river(" + riverName().getType() + "/" + riverName().getName() + ")")
                .newThread(riverLogic);
        this.riverThread.start();
    }

    @Override
    public void close() {
        logger.info("CLOSE GitlabRiver");
        this.closed = true;
        this.riverThread.interrupt();
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
