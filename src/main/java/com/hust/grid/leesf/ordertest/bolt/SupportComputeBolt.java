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
 * 计算商品对的支持度
 * 
 * @author leesf
 *
 */
public class SupportComputeBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	private Map<ItemPair, Integer> pairCounts; // 存储商品对及其出现的次数
	private int pairTotalCount; // 商品对总数

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		pairCounts = new HashMap<ItemPair, Integer>();
		pairTotalCount = 0;
	}

	/**
	 * 由于SupportComputeBolt订阅了多个流，其需要根据不同的字段做出不同的行为
	 */
	public void execute(Tuple tuple) {
		if (tuple.getFields().get(0).equals(FieldNames.TOTAL_COUNT)) { // 对应PairTotalCountBolt
			// 取出商品对总数量
			pairTotalCount = tuple.getIntegerByField(FieldNames.TOTAL_COUNT);
		} else if (tuple.getFields().size() == 3) { // 对应PairCountBolt
			// 取出商品及其商品对出现的次数
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			int pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);
			// 存储商品对及其次数
			pairCounts.put(new ItemPair(item1, item2), pairCount);
		} else if (tuple.getFields().get(0).equals(FieldNames.COMMAND)) { // 对应CommandSpout
			for (ItemPair itemPair : pairCounts.keySet()) { // 遍历商品对
				// 计算商品支持度，使用商品对出现的次数除以商品对总数量
				double itemSupport = (double) (pairCounts.get(itemPair).intValue()) / pairTotalCount;

				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemSupport));
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 定义元组字段
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.SUPPORT));
	}

}
