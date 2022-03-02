package customerManagement;

import java.time.Duration;
import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;

import org.apache.avro.specific.SpecificData;
import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import customerManagement.avro.Customer;

public class CustomerReader {
    private final Logger LOG = Logger.getLogger(CustomerReader.class);
    private final String topic;
    private final boolean isAsync;
    private final KafkaConsumer<Integer,Customer> consumer;
    private final HashMap<String,Integer> customerMap;

    public CustomerReader(String t,Boolean isA){
        topic = t;
        isAsync = isA;

        Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.102:9092,192.168.0.103:9092");
        prop.put("key.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
        prop.put("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
        //Consumer must have group id
        prop.put("group.id","customerConsumer");
        //Consumer must have schema.registry.uil
        prop.put("schema.registry.url","http://192.168.0.103:8081");
        consumer = new KafkaConsumer<>(prop);
        customerMap = new HashMap<>();
    }

    public void run(){
        // consumer 订阅一系列topic，可以使用正则表达式，比如
        // consumer.subscribe("test_*");
        consumer.subscribe(Collections.singleton(topic));
        try{
            while(isAsync){
                //KIP-266: poll(long) is deprecated because it may block indefinitely
                ConsumerRecords<Integer,Customer> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<Integer,Customer> record: records){
                    LOG.debug(String.format("topic = %s, partition = %s, offset = %d, customer = %s, name = %s\n",
                            record.topic(),record.partition(),record.offset(),record.key(),record.value()));
                    int newValue = 1;
                    Customer next = (Customer) SpecificData.get().deepCopy(Customer.SCHEMA$,record.value());
                    //genericRecord$ can't be cast to Customer
                    if(customerMap.containsKey(next.getName().toString())){
                        newValue = customerMap.get(next.getName().toString()) + 1;
                    }
                    customerMap.put(next.getName().toString(),newValue);
                    JSONObject json = new JSONObject(customerMap);
                    System.out.println(json.toString(4));
                }
            }
        }finally{
            //always close consumer
            consumer.close();
        }
    }
    public static void main(String[] args){
        CustomerReader reader = new CustomerReader("test_customer",true);
        reader.run();
    }
}
