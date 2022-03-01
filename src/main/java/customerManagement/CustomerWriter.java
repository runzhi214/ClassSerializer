package customerManagement;

import customerManagement.avro.Customer;
import org.apache.log4j.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class CustomerWriter extends Thread{
    private static Logger LOG = Logger.getLogger(CustomerWriter.class);
    private final Boolean isAsync;
    private final String topic;
    private final KafkaProducer<Integer,Customer> producer;

    public CustomerWriter(String topic,Boolean isAsync){
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.0.102:9092,192.168.0.103:9092");
        props.put("acks","all");
        props.put("key.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url","http://192.168.0.103:8081");//my schema registry server

        producer = new KafkaProducer<Integer, Customer>(props);
        this.isAsync = isAsync;
        this.topic = topic;
    }

    public void run(){
        while(true){
            Customer customer = new CustomerGenerator().getNext();
            System.out.println("Generated customer "+ customer.toString());
            ProducerRecord<Integer,Customer> record = new ProducerRecord<>(topic,customer);//no key
            producer.send(record);
        }
    }

    public static void main(String[] args){
        CustomerWriter cusWriter = new CustomerWriter("test_customer", false);
        cusWriter.start();
    }
}
