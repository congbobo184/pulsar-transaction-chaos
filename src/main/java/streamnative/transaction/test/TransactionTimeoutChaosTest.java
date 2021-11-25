package streamnative.transaction.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.RateLimiter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class TransactionTimeoutChaosTest  {


    private final static String TENANT = "transaction";
    private final static String NAMESPACE = "chaos-test";

    final long produceCount = 1000;
    final static long produceRate = 500;

    private Consumer<Long> consumer;

    private Producer<Long> producer;

    private final PulsarClient client;

    private final PulsarAdmin admin;

    private Producer<Long> transactionProducer;

    private final RateLimiter rateLimiter = RateLimiter.builder().permits(produceRate).rateTime(1).timeUnit(TimeUnit.SECONDS).build();

    private static final String TOPIC_PREFIX = "transaction-timeout-chaos-test";
    private static final String topicName = TopicName.get(TopicDomain.persistent.toString(),
            NamespaceName.get(TENANT, NAMESPACE), TOPIC_PREFIX).toString() + RandomUtils.nextLong();
    private static final ExecutorService executor = Executors.newFixedThreadPool(2);
    private static final AtomicLong sendValue = new AtomicLong();

    public TransactionTimeoutChaosTest(String pulsarServiceUrl, String pulsarAdminUrl) throws Throwable {
        this.client = PulsarClient.builder()
                .serviceUrl(pulsarServiceUrl).enableTransaction(true).build();
        this.admin = PulsarAdmin.builder().serviceHttpUrl(pulsarAdminUrl).build();
        doSetup();
        testTimeoutTransactionMessage();
    }

    protected void doSetup() throws Exception {
        createTenantIfNotExisted(TENANT);
        createNamespaceIfNotExisted(TENANT, NamespaceName.get(TENANT, NAMESPACE));
        List<String> topics = admin.namespaces().getTopics(NamespaceName.get(TENANT, NAMESPACE).toString());
        for (String topic : topics) {
            if (topic.contains(TOPIC_PREFIX)) {
                admin.topics().delete(topic);
            }
        }
        consumer = client
                .newConsumer(Schema.INT64)
                .topic(topicName)
                .subscriptionName("chaos-sub")
                .messageListener(new TransactionTimeoutListener())
                .subscribe();
        producer = client
                .newProducer(Schema.INT64)
                .topic(topicName)
                .producerName("ahahah")
                .enableBatching(false)
                .blockIfQueueFull(true)
                .maxPendingMessages(3000)
                .create();

        transactionProducer = client
                .newProducer(Schema.INT64)
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topicName)
                .enableBatching(false)
                .create();
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
                produceMsg(producer, produceCount, countDownLatch);
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

    private void produceMsg(Producer<Long> producer, long size, CountDownLatch countDownLatch)  {
        for (long i = 0; i < size; i++) {
            TypedMessageBuilder<Long> message = producer.newMessage().value(sendValue.incrementAndGet());
            while (true) {
                if (tryAcquire()) {
                    try {
                        message.send();
                        break;
                    } catch (PulsarClientException ignored) {
                        log.error("send message error", ignored);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        countDownLatch.countDown();
    }


    private void produceTransactionMsg(Producer<Long> producer, long size, CountDownLatch countDownLatch) throws Exception {
        Transaction transaction = client.newTransaction()
                .withTransactionTimeout(RandomUtils.nextLong(2, 31), TimeUnit.SECONDS).build().get();
        for (long i = 0; i < size; i++) {
            if (tryAcquire()) {
                producer.newMessage(transaction).value(-1L).sendAsync();
            }
        }
        producer.flushAsync();
        countDownLatch.countDown();
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

    public void createTenantIfNotExisted(String tenant) throws Exception {
        List<String> tenants = admin.tenants().getTenants();
        if (!tenants.contains(tenant)) {
            Set<String> clusters = new HashSet<>();
            clusters.addAll(admin.clusters().getClusters());
            Set<String> roles = new HashSet<>();
            roles.add("super-user");
            admin.tenants().createTenant(tenant,
                    TenantInfo.builder().adminRoles(roles).allowedClusters(clusters).build());
        }
    }

    public void createNamespaceIfNotExisted(String tenant, NamespaceName nn) throws Exception {
        List<String> namespaces = admin.namespaces().getNamespaces(tenant);
        if (!namespaces.contains(nn.toString())) {
            Set<String> clusters = new HashSet<>();
            clusters.addAll(admin.clusters().getClusters());
            admin.namespaces().createNamespace(nn.toString(), clusters);
        }
    }
}
