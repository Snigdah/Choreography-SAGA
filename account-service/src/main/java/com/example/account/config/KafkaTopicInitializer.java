package com.example.account.config;

import com.example.account.messaging.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Creates the Kafka topics this service needs, at startup, BEFORE the Kafka listeners start.
 * <ol>
 *   <li>List the topics that already exist on the broker</li>
 *   <li>Create only the missing ones (another service may create the same topic at the same moment - handled)</li>
 *   <li>Verify every topic is ready and has the expected partition count</li>
 * </ol>
 * If Kafka is not reachable, the service refuses to start with a clear message.
 */
@Slf4j
@Component
public class KafkaTopicInitializer implements SmartInitializingSingleton {

    private static final long TIMEOUT_SECONDS = 30;
    private static final int VERIFY_ATTEMPTS = 5;

    private final KafkaAdmin kafkaAdmin;
    private final int partitions;
    private final short replicas;

    public KafkaTopicInitializer(KafkaAdmin kafkaAdmin,
                                 @Value("${saga.kafka.topic.partitions:3}") int partitions,
                                 @Value("${saga.kafka.topic.replicas:1}") short replicas) {
        this.kafkaAdmin = kafkaAdmin;
        this.partitions = partitions;
        this.replicas = replicas;
    }

    @Override
    public void afterSingletonsInstantiated() {
        List<String> required = KafkaTopics.USED_BY_THIS_SERVICE;
        log.info("[KAFKA] Checking {} topic(s) used by this service: {}", required.size(), required);

        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> existing = admin.listTopics().names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            List<NewTopic> missing = new ArrayList<>();
            for (String topic : required) {
                if (existing.contains(topic)) {
                    log.info("[KAFKA] Topic '{}' already exists - not creating", topic);
                } else {
                    missing.add(new NewTopic(topic, partitions, replicas));
                }
            }

            createTopics(admin, missing);
            verifyTopics(admin, required);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while preparing Kafka topics", e);
        } catch (ExecutionException | TimeoutException e) {
            Object servers = kafkaAdmin.getConfigurationProperties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
            throw new IllegalStateException("Kafka is not reachable at " + servers
                    + ". Start it first:  cd kafka  &&  docker compose up -d", e);
        }
    }

    private void createTopics(AdminClient admin, List<NewTopic> topics)
            throws ExecutionException, InterruptedException, TimeoutException {
        if (topics.isEmpty()) {
            return;
        }
        CreateTopicsResult result = admin.createTopics(topics);
        for (NewTopic topic : topics) {
            try {
                result.values().get(topic.name()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                log.info("[KAFKA] Topic '{}' CREATED (partitions={}, replicas={})",
                        topic.name(), topic.numPartitions(), topic.replicationFactor());
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log.info("[KAFKA] Topic '{}' was just created by another service - OK", topic.name());
                } else {
                    throw e;
                }
            }
        }
    }

    /** A freshly created topic can take a moment to appear in metadata, so retry a few times. */
    private void verifyTopics(AdminClient admin, List<String> topics)
            throws ExecutionException, InterruptedException, TimeoutException {
        for (int attempt = 1; ; attempt++) {
            try {
                Map<String, TopicDescription> descriptions =
                        admin.describeTopics(topics).allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                descriptions.forEach(this::logTopic);
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException && attempt < VERIFY_ATTEMPTS) {
                    log.info("[KAFKA] Topics not visible yet, retrying ({}/{})", attempt, VERIFY_ATTEMPTS);
                    TimeUnit.SECONDS.sleep(1);
                } else {
                    throw e;
                }
            }
        }
    }

    private void logTopic(String name, TopicDescription description) {
        int actualPartitions = description.partitions().size();
        int actualReplicas = description.partitions().get(0).replicas().size();
        if (actualPartitions != partitions) {
            // All saga topics should have the SAME partition count, so one transactionRef
            // maps to the same partition number on every topic (see RangeAssignor in application.yml).
            log.warn("[KAFKA] Topic '{}' has {} partition(s) but {} expected - keep all saga topics equal",
                    name, actualPartitions, partitions);
        }
        log.info("[KAFKA] Topic '{}' READY (partitions={}, replicas={})", name, actualPartitions, actualReplicas);
    }
}
