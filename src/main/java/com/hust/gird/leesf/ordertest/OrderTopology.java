package com.hust.gird.leesf.ordertest;

import com.hust.grid.leesf.ordertest.bolt.ConfidenceComputeBolt;
import com.hust.grid.leesf.ordertest.bolt.FilterBolt;
import com.hust.grid.leesf.ordertest.bolt.PairCountBolt;
import com.hust.grid.leesf.ordertest.bolt.PairTotalCountBolt;
import com.hust.grid.leesf.ordertest.bolt.SplitBolt;
import com.hust.grid.leesf.ordertest.bolt.SupportComputeBolt;
import com.hust.grid.leesf.ordertest.common.ConfKeys;
import com.hust.grid.leesf.ordertest.common.FieldNames;
import com.hust.grid.leesf.ordertest.spout.CommandSpout;
import com.hust.grid.leesf.ordertest.spout.OrderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * 构造计算拓扑结构
 * 
 * @author leesf
 *
 */
public class OrderTopology {
	TopologyBuilder builder = new TopologyBuilder();
	Config conf = new Config();
	LocalCluster localCluster;

	public OrderTopology() {
		// 开启5个线程（任务）
		builder.setSpout("orderSpout", new OrderSpout(), 5);
		// CommandSpout计算任务简单，开启1个线程
		builder.setSpout("commandSpout", new CommandSpout(), 1);

		// 开启5个线程（任务）
		// 对OrderSpout按照字段分组，即根据字段（订单ID）进行分组，
		// 保证同一订单会被发送至同一个SplitBolt处理
		builder.setBolt("splitBolt", new SplitBolt(), 5).fieldsGrouping("orderSpout", new Fields(FieldNames.ID));

		// 开启5个线程（任务）
		// 对SplitBolt按照字段分组，即根据字段（商品对）进行分组
		// 保证同一商品对会被发送至同一个PairCountBolt处理
		builder.setBolt("pairCountBolt", new PairCountBolt(), 5).fieldsGrouping("splitBolt",
				new Fields(FieldNames.ITEM1, FieldNames.ITEM2));

		// 开启1个线程（任务）
		// 对SplitBolt按照全局分组，其发射的所有元组都将由最小任务ID进行处理
		builder.setBolt("pairTotalCountBolt", new PairTotalCountBolt(), 1).globalGrouping("splitBolt");

		// 开启5个线程（任务）
		// 对PairTotalCountBolt使用广播分组，将所有元组复制发送至所有的线程处理
		// 对PairCountBolt按照字段分组，保证相同字段（同一商品对）发送至同一线程处理
		// 对CommandBolt使用广播分组，将所有元组复制发送至所有的线程处理
		builder.setBolt("supportComputeBolt", new SupportComputeBolt(), 5).allGrouping("pairTotalCountBolt")
				.fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2))
				.allGrouping("commandSpout");

		// 开启5个线程（任务）
		// 对PairCountBolt按照字段分组，保证相同字段（同一商品对）发送至同一线程处理
		// 对commandSpout使用广播分组，将所有元组复制发送至所有的线程处理
		builder.setBolt("confidenceComputeBolt", new ConfidenceComputeBolt(), 5)
				.fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2))
				.allGrouping("commandSpout");

		// 默认开启1个线程（任务）
		// 对SupportComputeBolt按照字段分组
		// 对ConfidenceComputeBolt按照字段分组
		builder.setBolt("filterBolt", new FilterBolt())
				.fieldsGrouping("supportComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2))
				.fieldsGrouping("confidenceComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
	}

	public TopologyBuilder getBuilder() {
		return builder;
	}

	public LocalCluster getCluster() {
		return localCluster;
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		OrderTopology topology = new OrderTopology();

		if (args != null && args.length > 1) {
			topology.runCluster(args[0], args[1]);
		} else {
			topology.runLocal(10000);
		}

	}

	private void runLocal(int runTime) {
		conf.setDebug(true);
		conf.put(ConfKeys.REDIS_HOST, "localhost");
		conf.put(ConfKeys.REDIS_PORT, "6379");
		localCluster = new LocalCluster();
		localCluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutdownLocal();
		}
	}

	private void shutdownLocal() {
		if (localCluster != null) {
			localCluster.killTopology("test");
			localCluster.shutdown();
		}
	}

	private void runCluster(String name, String redisHost) throws AlreadyAliveException, InvalidTopologyException {
		conf.setDebug(true);
		conf.put(ConfKeys.REDIS_HOST, redisHost);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}
}
