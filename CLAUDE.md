# HadoopConnector Module — HDFS Integration

## Purpose
Provides Water Framework integration with Apache Hadoop HDFS (Hadoop Distributed File System). Exposes file system operations (upload, download, append, create folder, delete, check existence) through a `SystemApi` interface, meaning all operations bypass the permission system and are intended for trusted internal services (data pipelines, ETL, large file storage). Backed by Apache Hadoop 3.3.5 client.

## Sub-modules

| Sub-module | Runtime | Key Classes |
|---|---|---|
| `HadoopConnector-api` | All | `HadoopConnectorSystemApi`, `HadoopOptions` |
| `HadoopConnector-service` | Water/OSGi | `HadoopConnectorSystemServiceImpl`, `HadoopOptionsImpl` |

## Why SystemApi Only?
HDFS operations are infrastructure-level system calls with no concept of per-user permissions or entity ownership in the Water sense. All callers are trusted internal services. If user-level access control is needed, implement it at the business service layer that wraps the connector.

## HadoopConnectorSystemApi

```java
public interface HadoopConnectorSystemApi extends SystemApi {

    // Upload a local file to HDFS
    void upload(File localFile, String hdfsPath, boolean deleteSource);

    // Download from HDFS as stream
    InputStream download(String hdfsPath);

    // Open output stream for appending to existing HDFS file
    OutputStream appendToFile(String hdfsPath);

    // Directory operations
    void createFolder(String hdfsPath);
    void deleteFile(String hdfsPath);
    void deleteFolder(String hdfsPath);     // recursive delete

    // Existence check
    boolean pathExists(String hdfsPath);

    // File listing
    List<String> listFiles(String hdfsPath);
    List<String> listFolders(String hdfsPath);
}
```

## HadoopConnectorSystemServiceImpl

Core implementation using Hadoop `FileSystem` API:

```java
@FrameworkComponent
public class HadoopConnectorSystemServiceImpl implements HadoopConnectorSystemApi {
    @Inject @Setter private HadoopOptions hadoopOptions;

    private FileSystem fileSystem;

    @OnActivate
    void init() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopOptions.getHdfsUrl());
        fileSystem = FileSystem.get(conf);
    }

    @OnDeactivate
    void destroy() throws IOException {
        if (fileSystem != null) fileSystem.close();
    }
}
```

## HadoopOptions

Configuration interface for HDFS connection parameters:

```java
public interface HadoopOptions {
    String getHdfsUrl();            // e.g., "hdfs://localhost:8020"
    String getHdfsUser();           // OS user for HDFS operations (optional)
    int getReplicationFactor();     // default: 1 (test), 3 (production)
    int getBlockSizeMB();           // default: 128
}
```

`HadoopOptionsImpl` reads from `ApplicationProperties`:
```properties
water.connectors.hadoop.url=hdfs://localhost:8020
water.connectors.hadoop.user=hadoop
water.connectors.hadoop.replication=3
water.connectors.hadoop.block.size.mb=128
```

## Usage Pattern

```java
@FrameworkComponent
public class DataIngestionService {
    @Inject @Setter private HadoopConnectorSystemApi hadoopConnector;

    public void ingestFile(File localFile, String userId) {
        String hdfsPath = "/data/ingestion/" + userId + "/" + localFile.getName();

        // Upload
        hadoopConnector.upload(localFile, hdfsPath, false);

        // Verify
        if (hadoopConnector.pathExists(hdfsPath)) {
            // Process...
        }

        // Read back
        try (InputStream is = hadoopConnector.download(hdfsPath)) {
            // stream processing...
        }
    }

    public void appendLog(String logPath, String logLine) {
        try (OutputStream os = hadoopConnector.appendToFile(logPath)) {
            os.write((logLine + "\n").getBytes(StandardCharsets.UTF_8));
        }
    }
}
```

## No REST Endpoints
`HadoopConnector` does **not** expose REST endpoints. It is a pure system-level integration component accessed programmatically from other services.

## Dependencies
- `it.water.core:Core-api` — `SystemApi`, `@FrameworkComponent`, `@OnActivate`, `@OnDeactivate`, `@Inject`
- `org.apache.hadoop:hadoop-client:3.3.5` — HDFS FileSystem API
- `org.apache.hadoop:hadoop-minicluster:3.3.5` — in-memory HDFS for testing (test scope only)

## Testing

Uses `MiniDFSCluster` for in-memory HDFS without needing a real Hadoop cluster:

```java
@ExtendWith(WaterTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HadoopConnectorTest implements Service {
    @Inject @Setter private HadoopConnectorSystemApi hadoopConnector;

    private MiniDFSCluster miniDfsCluster;

    @BeforeAll
    void setupHdfs() throws IOException {
        Configuration conf = new Configuration();
        miniDfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        miniDfsCluster.waitActive();
        // Configure HadoopOptions to point to miniDfsCluster.getURI()
    }

    @AfterAll
    void teardownHdfs() {
        if (miniDfsCluster != null) miniDfsCluster.shutdown();
    }

    @Test
    void uploadAndDownloadShouldWork() throws IOException {
        File tempFile = File.createTempFile("test", ".txt");
        Files.write(tempFile.toPath(), "test content".getBytes());

        hadoopConnector.upload(tempFile, "/test/file.txt", false);
        Assertions.assertTrue(hadoopConnector.pathExists("/test/file.txt"));

        try (InputStream is = hadoopConnector.download("/test/file.txt")) {
            String content = new String(is.readAllBytes());
            Assertions.assertEquals("test content", content);
        }
    }
}
```

## Code Generation Rules
- `HadoopConnectorSystemApi` is a `SystemApi` — no permission annotations needed, no security context required
- Always close `InputStream` and `OutputStream` from HDFS operations (use try-with-resources)
- `deleteFolder()` is recursive — use with caution in production; add confirmation logic at the calling layer
- Custom `HadoopOptions` implementations: extend `HadoopOptionsImpl` or implement `HadoopOptions` directly, register with `@FrameworkComponent`
- For large files (> JVM heap): always stream via `InputStream`/`OutputStream` — never load into byte arrays
- No REST controllers in this module — no Karate tests needed
