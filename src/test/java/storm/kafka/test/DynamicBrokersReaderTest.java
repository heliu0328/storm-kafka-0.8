package storm.kafka.test;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import backtype.storm.Config;

import storm.kafka.DynamicBrokersReader;

public class DynamicBrokersReaderTest {
	@Test
	public void testGetBrokerInfo() {
		String zkStr = "10.3.255.222:2181";
		String zkPath = "/kafka08/brokers";
		String topic = "xapian_pet";
		
		Config conf = new Config();
		conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 20000);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 1000);
		DynamicBrokersReader reader = new DynamicBrokersReader(conf, zkStr, zkPath, topic);
		
		@SuppressWarnings("rawtypes")
		Map<String, List> brokerInfo = reader.getBrokerInfo();
		Assert.assertTrue(!brokerInfo.isEmpty());
	}
}
