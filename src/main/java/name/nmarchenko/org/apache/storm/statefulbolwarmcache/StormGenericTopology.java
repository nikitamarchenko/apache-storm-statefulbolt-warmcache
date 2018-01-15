package name.nmarchenko.org.apache.storm.statefulbolwarmcache;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("LinearIntegerSpout", new LinearIntegerSpout(20), 1);
        builder.setSpout("StateSourceSpout", new StateSourceSpout(), 1);
        builder.setBolt("StatefulReadBolt", new StatefulReadBolt(), 1)
                .noneGrouping("LinearIntegerSpout").noneGrouping("StateSourceSpout");

        return builder.createTopology();
    }
}
