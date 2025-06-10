package it.water.connectors.hadoop;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import it.water.connectors.hadoop.api.HadoopConnectorSystemApi;
import it.water.connectors.hadoop.api.options.HadoopOptions;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.service.Service;
import it.water.core.interceptors.annotations.Inject;
import it.water.core.registry.model.ComponentConfigurationFactory;
import it.water.core.testing.utils.junit.WaterTestExtension;
import lombok.Setter;

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
        componentRegistry.registerComponent(HadoopOptions.class, hadoopOptions, ComponentConfigurationFactory.createNewComponentPropertyFactory().withPriority(2).build());
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
    void uploadFileShouldWork() {
        File exampleFile = new File(this.getClass().getClassLoader().getResource("tmp/example.txt").getFile());
        Assertions.assertDoesNotThrow(() -> this.hadoopConnectorSystemApi.upload(exampleFile, "/dest/example.txt", true));
    }


    @Test
    @Order(3)
    void uploadFileShouldFailIfPathAlreadyExistsAsADirectory() throws IOException {
        // Test will be runs if docker image has been launched.
        // Please runs "docker-compose -f docker-compose-svil-hdfs-only.yml up"
        File exampleFile = new File(this.getClass().getClassLoader().getResource("tmp/example.txt").getFile());
        Assertions.assertThrows(IllegalStateException.class, () -> hadoopConnectorSystemApi.upload(exampleFile, "/dest/example.txt", true));
    }

    @Test
    @Order(4)
    void downloadFileShouldWork() throws IOException {
        File exampleFile = new File(this.getClass().getClassLoader().getResource("tmp/example.txt").getFile());
        ByteArrayInputStream fileInputStream = new ByteArrayInputStream(Files.readAllBytes(exampleFile.toPath()));
        String localContent = new String(fileInputStream.readAllBytes());
        InputStream hadoopIs = hadoopConnectorSystemApi.download("/dest/example.txt");
        ByteArrayInputStream bais = new ByteArrayInputStream(hadoopIs.readAllBytes());
        String hadoopFileContent = new String(bais.readAllBytes());
        Assertions.assertEquals(localContent, hadoopFileContent);
        hadoopIs.close();
    }

    @Test
    @Order(5)
    void appendToFileShouldWork() throws IOException {
        InputStream hadoopIs = hadoopConnectorSystemApi.download("/dest/example.txt");
        ByteArrayInputStream bais = new ByteArrayInputStream(hadoopIs.readAllBytes());
        String hadoopOriginalFileContent = new String(bais.readAllBytes());
        hadoopIs.close();
        OutputStream outputStream = hadoopConnectorSystemApi.appendToFile("/dest/example.txt");
        outputStream.write("--content-added--".getBytes());
        outputStream.flush();
        outputStream.close();
        hadoopIs = hadoopConnectorSystemApi.download("/dest/example.txt");
        bais = new ByteArrayInputStream(hadoopIs.readAllBytes());
        Assertions.assertEquals(hadoopOriginalFileContent+"--content-added--", new String(bais.readAllBytes()));
    }

    @Test
    @Order(6)
    void deleteFileShouldWork() throws IOException {
        Assertions.assertTrue(hadoopConnectorSystemApi.exists("/dest/example.txt"));
        hadoopConnectorSystemApi.deleteFile("/dest/example.txt");
        Assertions.assertFalse(hadoopConnectorSystemApi.exists("/dest/example.txt"));
    }

    @Test
    @Order(7)
    void createFodlerShouldWork() throws IOException {
        Assertions.assertFalse(hadoopConnectorSystemApi.exists("/dest/folder"));
        hadoopConnectorSystemApi.createFolder("/dest/folder");
        Assertions.assertTrue(hadoopConnectorSystemApi.exists("/dest/folder"));
        hadoopConnectorSystemApi.deleteFolder("/dest/folder");
        Assertions.assertFalse(hadoopConnectorSystemApi.exists("/dest/folder"));
    }
}
