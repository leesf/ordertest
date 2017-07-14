package com.hust.grid.leesf.ordertest.common;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import redis.clients.jedis.Jedis;

/**
 * 订单生成类
 * 
 * @author leesf
 */
public class OrderGenerator {
	private static String REDIS_HOST = "localhost";
	private static int REDIS_PORT = 6379;
	private static int ORDER_COUNT = 30;

	private static String[] ITEMS_NAME = new String[] { "milk", "coffee", "egg", "flower", "icecream", "wine", "water",
			"fish", "golf", "CD", "beer" };

	private static Jedis jedis;
	private static Random random;

	public static void main(String[] args) {
		prepareRandom();
		connectToRedis();
		pushTuples();
		disconnectFromRedis();
	}

	private static void prepareRandom() {
		random = new Random(1000);
	}

	private static void disconnectFromRedis() {
		jedis.disconnect();
	}

	@SuppressWarnings("unchecked")
	private static void pushTuples() {
		for (int i = 0; i < ORDER_COUNT; i++) { // 生成指定条数订单
			JSONObject orderTuple = new JSONObject();
			JSONArray items = new JSONArray();

			// 保存已生成的商品，避免在同一订单中生成重复商品
			Set<String> selectedItems = new HashSet<String>();

			for (int j = 0; j < 4; j++) { // 每一订单包含四种商品
				JSONObject item = new JSONObject();

				while (true) {
					int itemIndex = random.nextInt(ITEMS_NAME.length);
					String itemName = ITEMS_NAME[itemIndex];

					if (!selectedItems.contains(itemName)) { // 不同商品
						item.put(FieldNames.NAME, itemName); // 商品名称
						item.put(FieldNames.COUNT, random.nextInt(1000)); // 商品数量

						items.add(item);
						selectedItems.add(itemName);

						break;
					}
				}
			}

			orderTuple.put(FieldNames.ID, UUID.randomUUID().toString()); // 每一订单对应唯一的ID
			orderTuple.put(FieldNames.ITEMS, items); // 订单中的商品

			// 转化为JSON字符串并存入Redis
			String jsonText = orderTuple.toJSONString();
			jedis.rpush("orders", jsonText);
		}
	}

	private static void connectToRedis() {
		jedis = new Jedis(REDIS_HOST, REDIS_PORT);
		jedis.connect();
	}
}
