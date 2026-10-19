package org.apache.kafka.common.requests.transform;

import java.io.File;
import java.time.Duration;

import com.github.dockerjava.api.command.InspectContainerResponse;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;

/**
 * Single-node KRaft broker built from the fork's jars with this plugin installed (see resources/docker).
 * Jar locations and the base image come from system properties set by the integrationTest Gradle task.
 */
public class XformKafkaContainer extends GenericContainer<XformKafkaContainer> {
    private static final int PUBLIC_PORT = 9092;

    public XformKafkaContainer() {
        super(image());
        withExposedPorts(PUBLIC_PORT);
        waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
    }

    private final StringBuilder kafkaOpts = new StringBuilder();

    // appConfig reads <transformerName>-<key> system properties before the properties files.
    public XformKafkaContainer withTransformerConfig(String transformerName, String key, String value) {
        kafkaOpts.append(" -D").append(transformerName).append('-').append(key).append('=').append(value);
        return withEnv("KAFKA_OPTS", kafkaOpts.toString().trim());
    }

    public String getBootstrapServers() {
        return getHost() + ":" + getMappedPort(PUBLIC_PORT);
    }

    // The advertised address depends on the mapped port, which is only known once the container is created.
    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
        copyFileToContainer(Transferable.of("PLAINTEXT://" + getBootstrapServers() + ",BROKER://localhost:9093"), "/tmp/advertised.listeners");
    }

    private static ImageFromDockerfile image() {
        ImageFromDockerfile image = new ImageFromDockerfile("kafka-req-xform-http-it", true)
            .withBuildArg("KAFKA_BASE_IMAGE", required("xform.kafkaBaseImage"))
            .withFileFromClasspath("Dockerfile", "docker/Dockerfile")
            .withFileFromClasspath("entrypoint.sh", "docker/entrypoint.sh")
            .withFileFromClasspath("server.properties", "docker/server.properties")
            .withFileFromClasspath("config", "docker/config");
        addLibs(image, "broker-libs", required("xform.brokerLibs"));
        addLibs(image, "plugin-libs", required("xform.pluginLibs"));
        return image;
    }

    private static void addLibs(ImageFromDockerfile image, String dir, String paths) {
        for (String path : paths.split(File.pathSeparator)) {
            File file = new File(path);
            image.withFileFromFile(dir + "/" + file.getName(), file);
        }
    }

    private static String required(String property) {
        String value = System.getProperty(property);
        if (null == value || value.isBlank()) {
            throw new IllegalStateException("System property " + property + " is not set; run via ./gradlew integrationTest");
        }
        return value;
    }
}
