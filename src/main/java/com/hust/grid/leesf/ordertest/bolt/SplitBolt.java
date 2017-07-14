package com.hust.grid.leesf.ordertest.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * 对订单中的商品进行两两组合并发送
 * 
 * @author leesf
 *
 */
public class SplitBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	private Map<String, List<String>> orderItems; // 存储订单及其商品

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		orderItems = new HashMap<String, List<String>>();
	}

	public void execute(Tuple tuple) {
		// 获取订单号和商品名称
		String id = tuple.getStringByField(FieldNames.ID);
		String newItem = tuple.getStringByField(FieldNames.NAME);

		if (!orderItems.containsKey(id)) { // 不包含该订单
			// 新生商品链表
			ArrayList<String> items = new ArrayList<String>();
			// 添加商品
			items.add(newItem);

			orderItems.put(id, items);

			return;
		}
		// 包含订单，取出订单中包含的商品
		List<String> items = orderItems.get(id);
		for (String existItem : items) { // 遍历商品
			// 将元组中提取的商品与订单中已存在的商品组合后发射
			collector.emit(createPair(newItem, existItem));
		}
		// 添加新的商品
		items.add(newItem);
	}

	private Values createPair(String item1, String item2) { // 按照指定顺序生成商品对
		if (item1.compareTo(item2) > 0) {
			return new Values(item1, item2);
		}

		return new Values(item2, item1);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明元组字段
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
	}
}
