package org.devtools.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class GitlabService extends AbstractLifecycleComponent<GitlabService> {

    @Inject
    public GitlabService(final Settings settings) {
        super(settings);
        logger.info("CREATE GitlabService");

        // TODO Your code..
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START GitlabService");

        // TODO Your code..
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP GitlabService");

        // TODO Your code..
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE GitlabService");

        // TODO Your code..
    }

}
