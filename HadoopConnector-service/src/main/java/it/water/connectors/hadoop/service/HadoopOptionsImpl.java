package it.water.connectors.hadoop.service;

import it.water.connectors.hadoop.api.options.HadoopOptions;
import it.water.core.api.bundle.ApplicationProperties;
import it.water.core.interceptors.annotations.FrameworkComponent;
import it.water.core.interceptors.annotations.Inject;
import lombok.Setter;

@FrameworkComponent
public class HadoopOptionsImpl implements HadoopOptions {
    @Inject
    @Setter
    private ApplicationProperties applicationProperties;

    @Override
    public String getHadoopUrl() {
        return applicationProperties.getPropertyOrDefault("water.connectors.hadoop.url", "hdfs://localhost:8020");
    }
}
