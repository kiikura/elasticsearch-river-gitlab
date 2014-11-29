package org.devtools;

import java.util.Collection;

import org.devtools.module.GitlabModule;
import org.devtools.module.GitlabRiverModule;
import org.devtools.rest.GitlabRestAction;
import org.devtools.service.GitlabService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversModule;

public class GitlabPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "GitlabPlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-river-gitlab plugin.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(GitlabRestAction.class);
    }

    // for River
    public void onModule(final RiversModule module) {
        module.registerRiver("hello", GitlabRiverModule.class);
    }

    // for Service
    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(GitlabModule.class);
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists
                .newArrayList();
        services.add(GitlabService.class);
        return services;
    }
}
