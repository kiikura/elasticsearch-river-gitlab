package org.devtools.module;

import org.devtools.river.GitlabRiver;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class GitlabRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(GitlabRiver.class).asEagerSingleton();
    }
}
