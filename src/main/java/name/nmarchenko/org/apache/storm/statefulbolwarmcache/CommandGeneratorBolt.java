package name.nmarchenko.org.apache.storm.statefulbolwarmcache;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CommandGeneratorBolt extends BaseTickTupleAwareRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CommandGeneratorBolt.class);
    private final long msgIdMax = 40;
    private long msgId = 0;
    private OutputCollector _collector;

    public CommandGeneratorBolt() {
        LOG.info("CommandGeneratorBolt::ctor");
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        LOG.info("CommandGeneratorBolt::prepare");
        _collector = collector;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOG.info("CommandGeneratorBolt::getComponentConfiguration");
        return TupleUtils.putTickFrequencyIntoComponentConfig(null, 1);
    }

    protected void onTickTuple(Tuple tuple) {
        if (msgId < msgIdMax) {
            _collector.emit(tuple, new Values(Long.toString(msgId)));
            LOG.info("CommandGeneratorBolt::onTickTuple => send #" + msgId);
            msgId++;
        }
    }

    @Override
    protected void process(Tuple tuple) { }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer output) {
        LOG.info("CommandGeneratorBolt::declareOutputFields");
        output.declare(new Fields("message"));
    }
}
