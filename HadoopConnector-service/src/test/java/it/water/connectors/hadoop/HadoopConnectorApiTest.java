package it.water.connectors.hadoop;

import it.water.connectors.hadoop.api.HadoopConnectorSystemApi;
import it.water.connectors.hadoop.api.options.HadoopOptions;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.service.Service;
import it.water.core.interceptors.annotations.Inject;
import it.water.core.registry.model.ComponentConfigurationFactory;
import it.water.core.testing.utils.junit.WaterTestExtension;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Generated with Water Generator.
 * Test class for HadoopConnector Services.
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

    private HadoopOptions hadoopOptions;

    private MiniDFSCluster cluster;
    private FileSystem fs;

    @BeforeAll
    void setup() throws IOException {
        var baseDir = Files.createTempDirectory("hdfs-test");
        System.setProperty("test.build.data", baseDir.toAbsolutePath().toString());
        Configuration conf = new Configuration();
        conf.set("dfs.namenode.rpc-bind-host", "localhost");
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());
        // Using always hostnames
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("hadoop.security.token.service.use_ip", "false");
        cluster = new MiniDFSCluster.Builder(conf)
                .checkDataNodeAddrConfig(true)
                .nameNodeHttpPort(8020)
                .build();
        cluster.waitClusterUp();
        fs = FileSystem.get(conf);
        hadoopOptions = new HadoopOptionsImplOverride(fs.getUri().toString());
        componentRegistry.registerComponent(HadoopOptions.class,hadoopOptions, ComponentConfigurationFactory.createNewComponentPropertyFactory().withPriority(2).build());
    }

    @AfterAll
    void teardown() throws IOException {
        if (fs != null) fs.close();
        if (cluster != null) cluster.shutdown();
    }

    /**
     * Testing basic injection of basic component for hadoopconnector entity.
     */
    @Test
    @Order(1)
    void componentsInsantiatedCorrectly() {
        this.hadoopConnectorSystemApi = this.componentRegistry.findComponent(HadoopConnectorSystemApi.class, null);
        Assertions.assertNotNull(this.hadoopConnectorSystemApi);
    }

    @Test()
    @Order(2)
    public void copyFileShouldWork() throws IOException {
        File exampleFile = new File(this.getClass().getClassLoader().getResource("tmp/example.txt").getFile());
        hadoopConnectorSystemApi.upload(exampleFile, "/dest/example.txt", true);
    }


    @Test
    @Order(3)
    public void copyFileShouldFailIfPathAlreadyExistsAsADirectory() throws IOException {
        // Test will be runs if docker image has been launched.
        // Please runs "docker-compose -f docker-compose-svil-hdfs-only.yml up"
        File exampleFile = new File(this.getClass().getClassLoader().getResource("tmp/example.txt").getFile());
        hadoopConnectorSystemApi.upload(exampleFile, "/dest/example.txt", true);
    }


    @Test
    @Order(4)
    void deleteFileShouldWork() throws IOException {
        hadoopConnectorSystemApi.deleteFile("/tmp/example.txt");
    }
}
