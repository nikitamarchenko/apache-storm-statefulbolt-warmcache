/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package name.nmarchenko.org.apache.storm.statefulbolwarmcache;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class LinearIntegerSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(LinearIntegerSpout.class);
    private SpoutOutputCollector collector;
    private long msgId = 0;
    private long max = 0;


    public LinearIntegerSpout(long max)
    {
        LOG.info("LinearIntegerSpout ctor");
        this.max = max;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "value"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (msgId < max) {
            LOG.info("nextTuple: " + msgId);
            collector.emit(new Values("LinearIntegerSpout", msgId), msgId);
            ++msgId;
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.info("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Got FAIL for msgId : " + msgId);
        collector.emit(new Values("LinearIntegerSpout", msgId), msgId);
    }
}
