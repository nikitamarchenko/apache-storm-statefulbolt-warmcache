package name.nmarchenko.org.apache.storm.statefulbolwarmcache;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SourceOfTruthBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SourceOfTruthBolt.class);

    private OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        LOG.info("SourceOfTruthBolt::prepare");
    }

    @Override
    public void execute(Tuple input) {
        LOG.info("SourceOfTruthBolt::execute Send state data");
        _collector.emit(input, new Values("Some state Data"));
        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer output) {
        LOG.info("SourceOfTruthBolt::declareOutputFields");
        output.declare(new Fields("message"));
    }
}
