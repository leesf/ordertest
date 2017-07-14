package com.hust.grid.leesf.ordertest.spout;

import java.util.Map;

import com.hust.grid.leesf.ordertest.common.FieldNames;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 统计支持度和置信度
 * 
 * @author leesf
 */
public class CommandSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		// 休眠5S后发射“statistics”
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		collector.emit(new Values("statistics"));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明元组字段
		declarer.declare(new Fields(FieldNames.COMMAND));
	}
}
