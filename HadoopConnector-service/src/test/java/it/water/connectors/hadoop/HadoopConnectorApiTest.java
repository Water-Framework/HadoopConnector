package it.water.connectors.hadoop;

import it.water.connectors.hadoop.api.HadoopConnectorSystemApi;
import it.water.core.api.bundle.Runtime;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.service.Service;
import it.water.core.interceptors.annotations.Inject;
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

    @Inject
    @Setter
    private Runtime runtime;

    private static MiniDFSCluster cluster;
    private static FileSystem fs;

    @BeforeAll
    static void setup() throws IOException {
        var baseDir = Files.createTempDirectory("hdfs-test");
        System.setProperty("test.build.data", baseDir.toAbsolutePath().toString());
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());
        cluster = new MiniDFSCluster.Builder(conf).build();
        cluster.waitClusterUp();
        fs = FileSystem.get(conf);
    }

    @AfterAll
    static void teardown() throws IOException {
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
        hadoopConnectorSystemApi.copyFile(exampleFile, "/dest/example.txt", true);
    }


    @Test
    @Order(3)
    public void copyFileShouldFailIfPathAlreadyExistsAsADirectory() throws IOException {
        // Test will be runs if docker image has been launched.
        // Please runs "docker-compose -f docker-compose-svil-hdfs-only.yml up"
        File exampleFile = new File(this.getClass().getClassLoader().getResource("tmp/example.txt").getFile());
        hadoopConnectorSystemApi.copyFile(exampleFile, "/dest/example.txt", true);
    }


    @Test
    @Order(4)
    void deleteFileShouldWork() throws IOException {
        hadoopConnectorSystemApi.deleteFile("/tmp/example.txt");
    }
}
