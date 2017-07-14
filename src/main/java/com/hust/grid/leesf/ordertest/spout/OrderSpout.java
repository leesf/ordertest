package com.hust.grid.leesf.ordertest.spout;

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.hust.grid.leesf.ordertest.common.ConfKeys;
import com.hust.grid.leesf.ordertest.common.FieldNames;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

/**
 * 数据源，从redis读取订单
 * 
 * @author leesf
 *
 */
public class OrderSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private Jedis jedis;
	private String host;
	private int port;

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.host = conf.get(ConfKeys.REDIS_HOST).toString();
		this.port = Integer.parseInt(conf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	public void nextTuple() {
		String content = jedis.rpop("orders"); // 获取一条订单数据

		if (null == content || "nil".equals(content)) { // 若无，则等待300ms
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else { // 对订单数据进行转化
			JSONObject object = (JSONObject) JSONValue.parse(content);
			String id = object.get(FieldNames.ID).toString(); // 获取ID
			JSONArray items = (JSONArray) object.get(FieldNames.ITEMS); // 获取订单中的商品

			for (Object obj : items) { // 遍历订单中的商品
				JSONObject item = (JSONObject) obj;
				String name = item.get(FieldNames.NAME).toString(); // 商品名称
				int count = Integer.parseInt(item.get(FieldNames.COUNT).toString()); // 商品数量
				collector.emit(new Values(id, name, count)); // 发射订单号、商品名称、商品数量

				if (jedis.hexists("itemCounts", name)) { // redis中存在name字段
					jedis.hincrBy("itemCounts", name, 1); // 商品对应数量（订单中多个商品当作1个）增加1
				} else { // redis中不存在name字段
					jedis.hset("itemCounts", name, "1"); // 将name字段的值（商品数量）设置为1
				}
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明发射元组字段
		declarer.declare(new Fields(FieldNames.ID, FieldNames.NAME, FieldNames.COUNT));
	}
}
