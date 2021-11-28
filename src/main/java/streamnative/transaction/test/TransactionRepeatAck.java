package streamnative.transaction.test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import jdk.internal.net.http.common.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

@Slf4j
public class TransactionRepeatAck extends TransactionTestBase{

    protected Producer<Integer> producer;

    protected Consumer<Integer> consumer1;

    protected Consumer<Integer> consumer2;

    private static final String TOPIC_PREFIX = "transaction-repeatAck-test";

    private static final String topicName = TopicName.get(TopicDomain.persistent.toString(),
            NamespaceName.get(TENANT, NAMESPACE), TOPIC_PREFIX).toString() + RandomUtils.nextLong();

    public TransactionRepeatAck(String pulsarAdminUrl, String pulsarServiceUrl) throws Exception{
        super(PulsarAdmin.builder().serviceHttpUrl(pulsarAdminUrl).build(), pulsarServiceUrl);
        doSetup();
        testRepeatTest();
    }

    private void doSetup() throws Exception{
        internalSetup(TOPIC_PREFIX);
        admin.topics().createPartitionedTopic(topicName, 3);

        Map<String, Object> producerConf = new HashMap<>();
        producerConf.put("producerName", "testTimeout");
        producerConf.put("batchingEnabled", false);
        producerConf.put("blockIfQueueFull", true);
        producerConf.put("maxPendingMessages", 3000);
        producerConf.put("topicName", topicName);
        producer = internalBuildProduce(producerConf, Schema.INT32);

        Map<String, Object> consumerConf = new HashMap<>();
        Set<String> topicNames = Sets.newTreeSet();
        topicNames.add(topicName);
        consumerConf.put("topicNames", topicNames);
        consumer1 = internalBuildConsumer(null, consumerConf, Schema.INT32);

        consumer2 = internalBuildConsumer(null, consumerConf, Schema.INT32);
    }

    public void testRepeatTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            producer.newMessage().value(i).send();
        }
        while (true) {
            List<Message<Integer>> messages = new LinkedList<>();
            for (int i = 0; i < 100; ) {
                Message<Integer> message = consumer1.receive(5, TimeUnit.SECONDS);
                if (message != null && message.getValue() == i) {
                    messages.add(message);
                    i++;
                } else if (message == null) {
                    log.warn("receive message timeout");
                } else if (message.getValue() > i) {
                    log.error("Receive messages not in order, except value {}, actual value {}, messageId {}",
                            i, message.getValue(), message.getMessageId());
                    System.exit(-1);
                } else if (message.getValue() < i) {
                    log.warn("Receive messages repeatedly except value {}, actual value {}, messageId {}",
                            i, message.getValue(), message.getMessageId());
                }
            }
            Map<Pair<CompletableFuture<Void>, CompletableFuture<Void>>, Message<Integer>> futures = new LinkedHashMap<>();
            Transaction transaction1 = internalBuildTransaction(60 * 60);
            Transaction transaction2 = internalBuildTransaction(60 * 60);
            for (Message<Integer> message : messages) {
                Pair<CompletableFuture<Void>, CompletableFuture<Void>> pair = new Pair<>(
                        consumer1.acknowledgeAsync(message.getMessageId(), transaction1),
                        consumer1.acknowledgeAsync(message.getMessageId(), transaction2)
                );
                futures.put(pair, message);
            }

            for (Pair<CompletableFuture<Void>, CompletableFuture<Void>> pair : futures.keySet()) {
                pair.first.whenComplete((ignore, exception) -> {
                    pair.second.whenComplete((ignore1, throwable) -> {
                        if (throwable != null) {
                            if (exception != null) {
                                log.warn("Both consumers ack the message {} failed", futures.get(pair).getMessageId());
                            }
                        } else {
                            if (exception == null) {
                                log.error("Both consumer ack the message {} successfully",
                                        futures.get(pair).getMessageId());
                                System.exit(-1);
                            }
                        }
                    });
                });
            }

            while (true) {
                try {
                    transaction1.abort().get();
                    transaction2.abort().get();
                    break;
                } catch (Exception ignored) {
                }
            }
        }
    }
}
