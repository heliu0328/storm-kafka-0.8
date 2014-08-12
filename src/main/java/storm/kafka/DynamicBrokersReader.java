package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class DynamicBrokersReader {
    
    CuratorFramework _curator;
    String _zkPath;
    String _topic;
    
    public DynamicBrokersReader(Map conf, String zkStr, String zkPath, String topic) {
        try {
            _zkPath = zkPath;
            _topic = topic;
            _curator = CuratorFrameworkFactory.newClient(
                    zkStr,
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                    15000,
                    new RetryNTimes(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
            _curator.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Map of host to List of port and number of partitions.
     * 
     * {"host1.mycompany.com" -> [9092, 5]}
     */
    public Map<String, List> getBrokerInfo() {     
        Map<String, List> ret = new HashMap();
        try {
            String topicBrokersPath = _zkPath + "/topics/" + _topic;
            String brokerInfoPath = _zkPath + "/ids";
            List<String> children = _curator.getChildren().forPath(topicBrokersPath);

            for(String c: children) {
                try {
                    List<String> numPartitionsDatas = _curator.getChildren().forPath(topicBrokersPath + "/" + c);
                    List<Integer> numPartitions = getNumPartitions(numPartitionsDatas);
                    for(Integer numpartition : numPartitions) {
                    	byte[] hostPortData = _curator.getData().forPath(brokerInfoPath + "/" + numpartition);
                    	HostPort hp = getBrokerHost(hostPortData);
                    	List<Object> info = new ArrayList<Object>();
                    	info.add((long)hp.port);
                    	info.add(numpartition);
                    	
                    	ret.put(hp.host, info);
                    }
                } catch(org.apache.zookeeper.KeeperException.NoNodeException e) {
                	e.printStackTrace();
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
    
    public void close() {
        _curator.close();
    }
    
    protected static HostPort getBrokerHost(byte[] contents) {
        try {
/*            String[] hostString = new String(contents, "UTF-8").split(":");
            String host = hostString[hostString.length - 2];
            int port = Integer.parseInt(hostString[hostString.length - 1]);*/
        	
        	JSONObject json = (JSONObject) JSONValue.parse(new String(contents,"UTF-8"));
        	
        	String host = json.get("host").toString();
        	int port = Integer.parseInt(json.get("port").toString());
            return new HostPort(host, port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    } 
    
    protected static List<Integer> getNumPartitions(List<String> partitions) {
    	List<Integer> intPartitions = new LinkedList<Integer>();
        try {
        	for(String partition : partitions) {
        		intPartitions.add(Integer.parseInt(partition));
        	}
            return intPartitions;           
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    } 
    
    protected static int getNumPartitions(byte[] contents) {
        try {
            return Integer.parseInt(new String(contents, "UTF-8"));            
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    } 
}
