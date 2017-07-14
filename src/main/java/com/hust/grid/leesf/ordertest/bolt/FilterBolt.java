package com.hust.grid.leesf.ordertest.bolt;

import java.util.Map;

import org.json.simple.JSONObject;

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
 * 过滤符合条件的商品对并存入redis
 * 
 * @author leesf
 *
 */
public class FilterBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	// 商品对的支持度和置信度
	private static final double SUPPORT_THRESHOLD = 0.01;
	private static final double CONFIDENCE_THRESHOLD = 0.01;

	private OutputCollector collector;

	private Jedis jedis;
	private String host;
	private int port;

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.host = conf.get(ConfKeys.REDIS_HOST).toString();
		this.port = Integer.parseInt(conf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
		// 取出商品并构造商品对
		String item1 = tuple.getStringByField(FieldNames.ITEM1);
		String item2 = tuple.getStringByField(FieldNames.ITEM2);
		ItemPair itemPair = new ItemPair(item1, item2);
		String pairString = itemPair.toString();

		double support = 0;
		double confidence = 0;

		if (tuple.getFields().get(2).equals(FieldNames.SUPPORT)) { // 对应SupportComputeBolt
			// 获取支持度并存入redis
			support = tuple.getDoubleByField(FieldNames.SUPPORT);
			jedis.hset("supports", pairString, String.valueOf(support));
		} else if (tuple.getFields().get(2).equals(FieldNames.CONFIDENCE)) { // 对应ConfidenceComputeBolt
			// 获取置信度并存入redis
			confidence = tuple.getDoubleByField(FieldNames.CONFIDENCE);
			jedis.hset("confidences", pairString, String.valueOf(confidence));
		}

		if (!jedis.hexists("supports", pairString) || !jedis.hexists("confidences", pairString)) { // 商品对的支持度和置信度还未计算完成，返回
			return;
		}
		// 商品对的支持度和置信度已经计算完成
		support = Double.parseDouble(jedis.hget("supports", pairString));
		confidence = Double.parseDouble(jedis.hget("confidences", pairString));

		if (support >= SUPPORT_THRESHOLD && confidence >= CONFIDENCE_THRESHOLD) { // 支持度和置信度超过阈值
			// 将该商品对信息存入redis中
			JSONObject pairValue = new JSONObject();
			pairValue.put(FieldNames.SUPPORT, support);
			pairValue.put(FieldNames.CONFIDENCE, confidence);

			jedis.hset("recommendedPairs", pairString, pairValue.toJSONString());

			collector.emit(new Values(item1, item2, support, confidence));
		} else { // 不高于阈值，则从redis中删除
			jedis.hdel("recommendedPairs", pairString);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明元组字段
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.SUPPORT, FieldNames.CONFIDENCE));
	}
}
