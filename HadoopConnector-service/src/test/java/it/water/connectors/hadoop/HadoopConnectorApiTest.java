package it.water.connectors.hadoop;

import it.water.connectors.hadoop.api.HadoopConnectorSystemApi;
import it.water.core.api.bundle.Runtime;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.service.Service;
import it.water.core.interceptors.annotations.Inject;
import it.water.core.testing.utils.junit.WaterTestExtension;
import lombok.Setter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Generated with Water Generator.
 * Test class for HadoopConnector Services.
 * <p>
 * Please use HadoopConnectorRestTestApi for ensuring format of the json response
 */
@ExtendWith(WaterTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HadoopConnectorApiTest implements Service {

    @Inject
    @Setter
    private ComponentRegistry componentRegistry;

    @Inject
    @Setter
    private HadoopConnectorSystemApi hadoopConnectorSystemApi;

    @Inject
    @Setter
    private Runtime runtime;

    /**
     * Testing basic injection of basic component for hadoopconnector entity.
     */
    @Test
    @Order(1)
    void componentsInsantiatedCorrectly() {
        this.hadoopConnectorSystemApi = this.componentRegistry.findComponent(HadoopConnectorSystemApi.class, null);
        Assertions.assertNotNull(this.hadoopConnectorSystemApi);
    }

}
