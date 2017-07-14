package com.hust.grid.leesf.ordertest.bolt;

import java.util.HashMap;
import java.util.Map;

import com.hust.grid.leesf.ordertest.common.ConfKeys;
import com.hust.grid.leesf.ordertest.common.FieldNames;
import com.hust.grid.leesf.ordertest.common.ItemPair;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

/**
 * 计算商品对的置信度
 * 
 * @author leesf
 */
public class ConfidenceComputeBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	private Map<ItemPair, Integer> pairCounts; // 存储商品对及其出现的次数

	private String host;
	private int port;
	private Jedis jedis;

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.host = conf.get(ConfKeys.REDIS_HOST).toString();
		this.port = Integer.parseInt(conf.get(ConfKeys.REDIS_PORT).toString());
		pairCounts = new HashMap<ItemPair, Integer>();
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	/**
	 * 由于ConfidenceComputeBolt订阅了多个流，其需要根据元组不同的字段做出不同的行为
	 */
	public void execute(Tuple tuple) {
		if (tuple.getFields().size() == 3) { // 对应PairCountBolt
			// 取出商品对及其出现次数
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			int pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);

			pairCounts.put(new ItemPair(item1, item2), pairCount);
		} else if (tuple.getFields().get(0).equals(FieldNames.COMMAND)) { // 对应CommandSpout，需要进行统计
			for (ItemPair itemPair : pairCounts.keySet()) { // 遍历商品对
				// 从redis中取出商品对中商品出现的次数
				double item1Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem1()));
				double item2Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem2()));
				double itemConfidence = pairCounts.get(itemPair).intValue();

				// 计算商品对置信度
				if (item1Count < item2Count) {
					itemConfidence /= item1Count;
				} else {
					itemConfidence /= item2Count;
				}

				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemConfidence));
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明元组字段
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.CONFIDENCE));
	}
}
