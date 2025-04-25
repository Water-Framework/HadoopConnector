package it.water.connectors.hadoop;

import it.water.connectors.hadoop.api.options.HadoopOptions;
import it.water.core.interceptors.annotations.FrameworkComponent;
import lombok.Getter;

/**
 * Overriding default component so we can set dynamic hadoop url
 */
public class HadoopOptionsImplOverride implements HadoopOptions {
    @Getter
    private String hadoopUrl;

    public HadoopOptionsImplOverride() {
    }

    public HadoopOptionsImplOverride(String hadoopUrl) {
        this.hadoopUrl = hadoopUrl;
    }
}
