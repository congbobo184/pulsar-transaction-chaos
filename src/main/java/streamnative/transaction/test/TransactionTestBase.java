package streamnative.transaction.test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.RateLimiter;

@Slf4j
public class TransactionTestBase {

    final static long produceRate = 500;
    protected final static String TENANT = "transaction";
    protected final static String NAMESPACE = "chaos-test";

    protected final String pulsarServiceUrl;

    protected final PulsarAdmin admin;

    protected final RateLimiter rateLimiter = RateLimiter.builder().permits(produceRate).rateTime(1).timeUnit(TimeUnit.SECONDS).build();

    public TransactionTestBase(PulsarAdmin admin, String pulsarServiceUrl) {
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.admin = admin;
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
    public void internalSetup(String TOPIC_PREFIX) throws Exception {
        createTenantIfNotExisted(TENANT);
        createNamespaceIfNotExisted(TENANT, NamespaceName.get(TENANT, NAMESPACE));
        List<String> topics = admin.namespaces().getTopics(NamespaceName.get(TENANT, NAMESPACE).toString());
        for (String topic : topics) {
            if (topic.contains(TOPIC_PREFIX)) {
                admin.topics().delete(topic);
            }
        }
    }

    public <T> Consumer<T> internalBuildConsumer(MessageListener<T> messageListener, Map<String,Object> configuration, Schema<T> schema)
            throws Exception {
        PulsarClient client = PulsarClient
                .builder()
                .serviceUrl(pulsarServiceUrl)
                .enableTransaction(true)
                .build();
        ConsumerBuilder<T> consumerBuilder = client
                .newConsumer(schema)
                .loadConf(configuration)
                .subscriptionName("chaos-sub" + RandomUtils.nextLong());
        if (messageListener != null) {
            consumerBuilder.messageListener(messageListener);
        }
        return consumerBuilder
                .subscribe();

    }

    public <T> Producer<T> internalBuildProduce(Map<String, Object> configuration, Schema<T> schema)
            throws Exception{
        PulsarClient client = PulsarClient
                .builder()
                .serviceUrl(pulsarServiceUrl)
                .enableTransaction(true)
                .build();

        ProducerConfig producerConfig = new ProducerConfig();
        client.newProducer().loadConf(configuration);
        ProducerBuilder<T> producerBuilder = client
                .newProducer(schema)
                .loadConf(configuration);

        return producerBuilder.create();

    }

    protected Transaction internalBuildTransaction(long timeout) throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarServiceUrl).enableTransaction(true).build();
        return client.newTransaction()
                .withTransactionTimeout(timeout, TimeUnit.SECONDS).build().get();
    }

    protected <T> void internalProduceMsg(Producer<T> producer, T value, RateLimiter rateLimiter, Transaction transaction) {
        TypedMessageBuilder<T> message;
        if (transaction != null) {
            message = producer.newMessage(transaction).value(value);
        } else {
            message = producer.newMessage().value(value);
        }
        while (true) {
            if (tryAcquire(rateLimiter)) {
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

    private boolean tryAcquire(RateLimiter rateLimiter) {
        while (!rateLimiter.tryAcquire()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignore) {
            }
        }
        return true;
    }
}
