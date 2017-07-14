package com.hust.grid.leesf.ordertest.bolt;

import java.util.Map;

import com.hust.grid.leesf.ordertest.common.FieldNames;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 计算商品对总数
 * 
 * @author leesf
 *
 */
public class PairTotalCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	private int totalCount; // 商品对总数

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		totalCount = 0;
	}

	public void execute(Tuple tuple) {
		totalCount++; // 每收到一个元组，便增加商品对总数
		collector.emit(new Values(totalCount)); // 发射商品对总数
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明元组字段
		declarer.declare(new Fields(FieldNames.TOTAL_COUNT));
	}
}
