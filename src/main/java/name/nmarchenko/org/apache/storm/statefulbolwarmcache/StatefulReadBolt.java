package name.nmarchenko.org.apache.storm.statefulbolwarmcache;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class StatefulReadBolt extends BaseStatefulBolt<KeyValueState<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(LinearIntegerSpout.class);

    KeyValueState<String, String> kvState;

    private OutputCollector collector;
    private boolean cacheIsEmpty = true;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        LOG.info("Execute");

        String type = input.getString(0);
        if (type.equals("StateSourceSpout")) {
            LOG.info("Got state");
            cacheIsEmpty = false;
            collector.ack(input);
        } else if (cacheIsEmpty) {
            LOG.info("Got Tuple from LinearIntegerSpout and mark as fail");
            collector.fail(input);
        } else {
            LOG.info("Got Tuple from LinearIntegerSpout #" + input.getLong(1));
            collector.ack(input);
        }
    }

    @Override
    public void initState(KeyValueState<String, String> state) {
        kvState = state;
    }
}