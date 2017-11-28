package hello;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;


@RestController
public class AccessCustomerController {

    @RequestMapping("/getById")
    public Customer getById(@RequestParam(value="id") String id) throws JsonProcessingException, UnknownHostException {
    	System.out.println("recieve id: " + id);
    	
    	TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
    			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("39.108.8.147"), 9300));
    	GetResponse res = client.prepareGet("customer", "doc", id).get();
    	
    	Map<String, Object> cust = res.getSource();
    	String cus_name = (String) cust.get("name");
    	client.close();
        return new Customer(cus_name, "bbbbb");
    }
    
    @RequestMapping("/searchByName")
    public List<Customer> searchByName(@RequestParam(value="name") String name) throws JsonProcessingException, UnknownHostException {
    	System.out.println("recieve name: " + name);
    	
    	TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
    			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("39.108.8.147"), 9300));
       	
    	SearchResponse res = client.prepareSearch("customer")
    			.setTypes("doc")
    			.setQuery(QueryBuilders.matchQuery("name", name))
    			.setExplain(true)
    			.get();

    	System.out.println("hit length: " + res.getHits().getHits().length);
    	
    	List<Customer> custs =  new ArrayList<Customer>();
    	
    	for(SearchHit hit: res.getHits().getHits())
    	{
    		Map<String, Object> cust = hit.getSource();
        	String cus_name = (String) cust.get("name");
    		Customer cus = new Customer(cus_name, "");
    		custs.add(cus);
   		}
    
    	client.close();

        return custs;
    }
    
    
    
    
}
