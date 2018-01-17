package name.nmarchenko.org.apache.storm.statefulbolwarmcache;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class StatefulBolt extends BaseStatefulBolt<KeyValueState<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulBolt.class);

    KeyValueState<String, String> kvState;

    private OutputCollector collector;
    private boolean cacheIsEmpty = true;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        LOG.info("StatefulBolt::prepare");
        this.collector = collector;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOG.info("CommandGeneratorBolt::getComponentConfiguration");
        /*
        I use tick here because we cant emit tuple without anchor in BaseStatefulBolt
        */
        return TupleUtils.putTickFrequencyIntoComponentConfig(null, 30);
    }

    @Override
    public void execute(Tuple input) {

        String source = input.getSourceComponent();

        LOG.info("StatefulBolt::execute source=" + source);

        if (TupleUtils.isTick(input) && cacheIsEmpty) {
            LOG.info("StatefulBolt::execute emit cache data request");
            collector.emit(input, new Values("cache data request"));
            return;
        }

        if (source.equals("READER_WARM_RESPONSE")) {
            LOG.info("StatefulBolt::execute Got state now can execute");
            LOG.info("=============================>>>INIT DONE<<<<=========================");
            cacheIsEmpty = false;
            collector.ack(input);
        } else if (cacheIsEmpty) {
            LOG.info("Got Tuple from SourceOfTruthBolt and mark as fail #" + input.getString(0));
            collector.fail(input);
        } else {
            LOG.info("Got Tuple from SourceOfTruthBolt #" + input.getString(0));
            collector.ack(input);
        }
    }

    @Override
    public void initState(KeyValueState<String, String> state) {
        kvState = state;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer output) {
        output.declare(new Fields("message"));
    }
}