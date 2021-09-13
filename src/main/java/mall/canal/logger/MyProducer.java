package mall.canal.logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/11/15:36
 * @Description:
 */
public class MyProducer {
    private static Producer myproducer;

    static {

        myproducer = getProducer();
    }

    public static Producer getProducer(){

        Properties properties = new Properties();

        properties.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String,String>(properties);
    }

    public static  void sendRecord(String topic,String value){

        myproducer.send(new ProducerRecord<String,String>(topic,value));

    }
}
