package streamnative.transaction.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

@Slf4j
public class TransactionTimeoutChaosTest  extends TransactionTestBase{


    final long produceCount = 1000;

    private Consumer<Long> consumer;

    private Producer<Long> producer;

    private Producer<Long> transactionProducer;

    private static final String TOPIC_PREFIX = "transaction-timeout-chaos-test";
    private static final String topicName = TopicName.get(TopicDomain.persistent.toString(),
            NamespaceName.get(TENANT, NAMESPACE), TOPIC_PREFIX).toString() + RandomUtils.nextLong();
    private static final ExecutorService executor = Executors.newFixedThreadPool(2);
    private static final AtomicLong sendValue = new AtomicLong();

    public TransactionTimeoutChaosTest(String pulsarServiceUrl, String pulsarAdminUrl) throws Throwable {
        super(PulsarAdmin.builder().serviceHttpUrl(pulsarAdminUrl).build(), pulsarServiceUrl);
        doSetup();
        testTimeoutTransactionMessage();
    }

    protected void doSetup() throws Exception {
        internalSetup(TOPIC_PREFIX);
        Map<String, Object> consumerConf = new HashMap<>();
        Set<String> topicNames = Sets.newTreeSet();
        topicNames.add(topicName);
        consumerConf.put("topicNames", topicNames);
        consumer = internalBuildConsumer(new TransactionTimeoutListener(), consumerConf, Schema.INT64);

        Map<String, Object> configuration = new HashMap<>();
        configuration.put("producerName", "testTimeout");
        configuration.put("batchingEnabled", false);
        configuration.put("blockIfQueueFull", true);
        configuration.put("maxPendingMessages", 3000);
        configuration.put("topicName", topicName);
        producer = internalBuildProduce(configuration,Schema.INT64);
        configuration.clear();
        configuration.put("sendTimeoutMs", 0);
        configuration.put("batchingEnabled", false);
        configuration.put("topicName", topicName);
        transactionProducer = internalBuildProduce(configuration, Schema.INT64);
    }

    static class TransactionTimeoutListener implements MessageListener<Long> {

        private final AtomicLong atomicLong = new AtomicLong();

        @Override
        public void received(Consumer<Long> consumer, Message<Long> msg) {
            Long value = msg.getValue();
            if (value == null) {
                /**
                 * produce: 1,2,3,4,5,
                 * consume: 1,2,2,3,4,4,5
                 */
                consumer.acknowledgeAsync(msg);
                return;
            }

            // messages not in order
            if (value > atomicLong.get() + 1) {
                log.error("receive message not in order, actual : {}, " +
                        "expect : {}, messageId : {}", value, atomicLong.get() + 1, msg.getMessageId());
                System.exit(0);
            } else if (value < atomicLong.get() + 1) {
                log.warn("receive message is smaller than expect, " +
                        "actual : {}, expect : {}, messageId : {}", value, atomicLong.get() + 1, msg.getMessageId());
            } else if (value == -1L) {
                log.error("receive transaction timeout message : {}, " +
                        "expect : {}, messageId : {}", value, atomicLong.get() + 1, msg.getMessageId());
            } else {
                atomicLong.incrementAndGet();
            }

            consumer.acknowledgeAsync(msg);
        }
    }

    public void testTimeoutTransactionMessage() throws Throwable {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        executor.execute(() -> {
            while (true) {
                internalProduceMsg(producer, sendValue.incrementAndGet(), rateLimiter, null);
            }
        });

        executor.execute(() -> {
            while (true) {
                try {
                    produceTransactionMsg(transactionProducer, produceCount, countDownLatch);
                } catch (Exception e) {
                    log.error("new transaction error", e);
                }
            }
        });
        countDownLatch.await();
    }


    private void produceTransactionMsg(Producer<Long> producer, long size, CountDownLatch countDownLatch) throws Exception {
        Transaction transaction = internalBuildTransaction(RandomUtils.nextLong(2, 31));
        if (tryAcquire()) {
            producer.newMessage(transaction).value(-1L).sendAsync();
        }
        producer.flushAsync();
    }

    private boolean tryAcquire() {
        while (!rateLimiter.tryAcquire()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignore) {
            }
        }
        return true;
    }


}
