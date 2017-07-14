package com.hust.grid.leesf.ordertest.bolt;

import java.util.HashMap;
import java.util.Map;

import com.hust.grid.leesf.ordertest.common.FieldNames;
import com.hust.grid.leesf.ordertest.common.ItemPair;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 计算商品对出现的次数
 * 
 * @author leesf
 *
 */
public class PairCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	private Map<ItemPair, Integer> pairCounts; // 存储商品对及其出现的次数

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.pairCounts = new HashMap<ItemPair, Integer>();
	}

	public void execute(Tuple tuple) {
		String item1 = tuple.getStringByField(FieldNames.ITEM1);
		String item2 = tuple.getStringByField(FieldNames.ITEM2);

		ItemPair itemPair = new ItemPair(item1, item2);
		int pairCount = 0;

		if (pairCounts.containsKey(itemPair)) { // 包含商品对
			// 取出商品对出现的次数
			pairCount = pairCounts.get(itemPair);
		}
		// 更新出现次数
		pairCount++;

		pairCounts.put(itemPair, pairCount);

		collector.emit(new Values(item1, item2, pairCount));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明元组字段
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.PAIR_COUNT));
	}
}
