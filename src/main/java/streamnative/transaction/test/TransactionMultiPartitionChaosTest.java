package streamnative.transaction.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.RateLimiter;

@Slf4j
public class TransactionMultiPartitionChaosTest extends TransactionTestBase{
    private Consumer<Long> consumer;

    private  Producer<Long> producer;

    private  Producer<Long> transactionProducer;

    final static long PRODUCE_RATE = 500;

    final static long SIZE_OF_TXN = 30;

    private final RateLimiter rateLimiter = RateLimiter.builder().permits(PRODUCE_RATE).rateTime(1)
            .timeUnit(TimeUnit.SECONDS).build();

    private static final String TOPIC_PREFIX = "transaction-multi-partition-test";

    private static final String topicName = TopicName.get(TopicDomain.persistent.toString(),
            NamespaceName.get(TENANT, NAMESPACE), TOPIC_PREFIX).toString() + RandomUtils.nextLong();

    private static final ExecutorService executor = Executors.newFixedThreadPool(2);

    public TransactionMultiPartitionChaosTest(String pulsarServiceUrl, String pulsarAdminUrl) throws Throwable{
        super(PulsarAdmin.builder().serviceHttpUrl(pulsarAdminUrl).build(), pulsarServiceUrl);
        doSetup();
        testSendMessagesToMultiPartitions();
    }

    protected void doSetup() throws Exception {
        internalSetup(TOPIC_PREFIX);
        admin.topics().createPartitionedTopic(topicName, 3);

        Map<String, Object> consumerConf = new HashMap<>();
        consumerConf.put("topicName", topicName);
        consumer = internalBuildConsumer(new TransactionMultiPartitionListener(), consumerConf, Schema.INT64);

        Map<String, Object> producerConf = new HashMap<>();
        producerConf.put("producerName", "testTimeout");
        producerConf.put("batchingEnabled", false);
        producerConf.put("blockIfQueueFull", true);
        producerConf.put("maxPendingMessages", 3000);
        producerConf.put("topicName", topicName);
        producer = internalBuildProduce(producerConf,Schema.INT64);

        Map<String, Object> txnProducerConf = new HashMap<>();
        txnProducerConf.put("sendTimeoutMs", 0);
        txnProducerConf.put("batchingEnabled", false);
        txnProducerConf.put("topicName", topicName);
        transactionProducer = internalBuildProduce(txnProducerConf, Schema.INT64);
    }

    static class TransactionMultiPartitionListener implements MessageListener<Long> {

        @Override
        public void received(Consumer<Long> consumer, Message<Long> msg) {
            Long value = msg.getValue();
            if (value == null) {
                /**
                 * common: 0
                 * aborted: -1L
                 * committed: 1L
                 */
                consumer.acknowledgeAsync(msg);
                return;
            }

            // receive aborted message
            if (value == -1L) {
                log.error("receive aborted message {}", msg.getMessageId().toString());
                System.exit(0);
            }
        }
    }

    public void testSendMessagesToMultiPartitions() throws Throwable {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        executor.execute(() -> {
            while (true) {
                internalProduceMsg(producer, 0L, rateLimiter, null);
            }
        });

        executor.execute(() -> {
            while (true) {
                try {
                    Transaction transaction = internalBuildTransaction(60L);
                    long value = RandomUtils.nextLong() % 2  == 0 ? -1L : 1L;
                    for (int i = 0; i < SIZE_OF_TXN; i++) {
                        internalProduceMsg(transactionProducer, value, rateLimiter, transaction);
                    }
                    Future<Void> future = value == -1 ? transaction.abort() : transaction.commit();
                    //Do not care about the result of the ending of txn.
                } catch (Exception e) {
                    log.error("Failed to build transaction");
                }
            }
        });
        countDownLatch.await();
    }


}
