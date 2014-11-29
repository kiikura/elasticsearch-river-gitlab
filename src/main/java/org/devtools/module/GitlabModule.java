package org.devtools.module;

import org.devtools.service.GitlabService;
import org.elasticsearch.common.inject.AbstractModule;

public class GitlabModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(GitlabService.class).asEagerSingleton();
    }
}