package name.nmarchenko.org.apache.storm.statefulbolwarmcache;

import kafka.api.OffsetRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class StormGenericTopology {

    public static final Logger LOG = LoggerFactory
            .getLogger(StormGenericTopology.class);

    private final TopologyProperties topologyProperties;

    public StormGenericTopology(TopologyProperties topologyProperties) {
        this.topologyProperties = topologyProperties;
    }

    public static void main(String[] args) throws Exception {
        String propertiesFile = args[0];
        TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
        StormGenericTopology topology = new StormGenericTopology(topologyProperties);
        topology.runTopology();
    }

    public void runTopology() throws Exception {

        StormTopology stormTopology = buildTopology();
        String stormExecutionMode = topologyProperties.getStormExecutionMode();

        switch (stormExecutionMode) {
            case ("cluster"):
                StormSubmitter.submitTopology(topologyProperties.getTopologyName(),
                        topologyProperties.getStormConfig(), stormTopology);
                break;
            case ("local"):
            default:
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyProperties.getTopologyName(),
                        topologyProperties.getStormConfig(), stormTopology);
                Thread.sleep(topologyProperties.getLocalTimeExecution());
                cluster.killTopology(topologyProperties.getTopologyName());
                cluster.shutdown();
                System.exit(0);
        }
    }

    private StormTopology buildTopology() {
        LOG.info("StormGenericTopology::buildTopology");

        TopologyBuilder builder = new TopologyBuilder();

//        builder.setSpout("LinearIntegerSpout", new LinearIntegerSpout(20), 1);
//        builder.setSpout("StateSourceSpout", new StateSourceSpout(), 1);
//        builder.setBolt("StatefulBolt", new StatefulBolt(), 1)
//                .noneGrouping("LinearIntegerSpout").noneGrouping("StateSourceSpout");

        /*

        SFB - StatefulBolt - Read CG_KS and SOT_KB
        SOT - SourceOfTruthBolt - Read CG_KS and SOT_KB contain state data
        WRITER_WARM_REQUEST - send messages to test.warm.request topic
        READER_WARM_REQUEST - read messages to test.warm.request topic
        WRITER_WARM_RESPONSE - send messages to test.warm.response topic
        READER_WARM_RESPONSE - read messages from test.warm.response topic

        CG - CommandGeneratorBolt - send tuple to CG_KB on tick
        WRITER_COMMAND - send messages to test.command topic
        READER_COMMAND - read messages to test.command topic
        */

        final String COMMAND_TOPIC = "test.command";
        final String WARM_REQUEST_TOPIC = "test.warm.request";
        final String WARM_RESPONSE_TOPIC = "test.warm.response";

        builder.setSpout("READER_COMMAND", createKafkaSpout(COMMAND_TOPIC, "READER_COMMAND"), 1);
        builder.setSpout("READER_WARM_REQUEST", createKafkaSpout(WARM_REQUEST_TOPIC, "READER_WARM_REQUEST"), 1);
        builder.setSpout("READER_WARM_RESPONSE", createKafkaSpout(WARM_RESPONSE_TOPIC, "READER_WARM_RESPONSE"), 1);

        builder.setBolt("SFB", new StatefulBolt(), 1)
                .noneGrouping("READER_COMMAND").noneGrouping("READER_WARM_RESPONSE");

        builder.setBolt("SOT", new SourceOfTruthBolt(), 1)
                .noneGrouping("READER_WARM_REQUEST");


        builder.setBolt("WRITER_WARM_REQUEST", createKafkaBolt(WARM_REQUEST_TOPIC))
                .noneGrouping("SFB");

        builder.setBolt("WRITER_WARM_RESPONSE", createKafkaBolt(WARM_RESPONSE_TOPIC))
                .noneGrouping("SOT");

        builder.setBolt("CG", new CommandGeneratorBolt(), 1);

        builder.setBolt("WRITER_COMMAND", createKafkaBolt(COMMAND_TOPIC))
                .noneGrouping("CG");

        return builder.createTopology();
    }

    protected org.apache.storm.kafka.KafkaSpout createKafkaSpout(String topic, String spoutId) {
        String zkRoot = "/"+topic;
        ZkHosts hosts = new ZkHosts(topologyProperties.getZookeeperHosts());

        SpoutConfig cfg = new SpoutConfig(hosts, topic, zkRoot, spoutId);
        cfg.startOffsetTime = OffsetRequest.EarliestTime();
        cfg.scheme = new SchemeAsMultiScheme(new StringScheme());
        cfg.bufferSizeBytes = 1024 * 1024 * 4;
        cfg.fetchSizeBytes = 1024 * 1024 * 4;

        return new org.apache.storm.kafka.KafkaSpout(cfg);
    }

    protected KafkaBolt<String, String> createKafkaBolt(final String topic) {
        return new KafkaBolt<String, String>()
                .withProducerProperties(makeKafkaProperties())
                .withTopicSelector(new DefaultTopicSelector(topic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String,String>());
    }

    private Properties makeKafkaProperties() {
        Properties kafka = new Properties();

        kafka.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafka.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafka.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        kafka.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "StormGenericTopology");
        kafka.setProperty("request.required.acks", "1");

        return kafka;
    }
}
