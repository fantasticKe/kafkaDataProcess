package cn.com.bonc.kafkaDataProcess.kafka;

import cn.com.bonc.kafkaDataProcess.util.Es;
import cn.com.bonc.kafkaDataProcess.util.PropertyReaderUtil;
import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.ho.yaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * @author maokeluo
 * @desc kafka消费者
 * @create 18-1-2
 */
public class MyKafkaConsumer {
    public static final Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class);

    private static KafkaConsumer<String, String> consumer;
    private final static AtomicBoolean closed =  new AtomicBoolean( false );
    private static Map<String,String> indexMap;

    static {
        PropertyReaderUtil reader = new PropertyReaderUtil();
        Map<String, String> serverMap = reader.readPropertyFile("kafkaServer.properties");
        Properties props = new Properties();
        serverMap.forEach((k,v)->props.put(k,v));
        consumer = new KafkaConsumer<>(props);
    }
    /**
     * @desc 消费数据
     * @author maokeluo
     * @methodName consumer
     * @param
     * @create 18-1-10
     * @return void
     */
    public static void consumData() {

       try {
           consumer.subscribe(Arrays.asList("output-stream"));
           //manualOffset(consumer);
           while (!closed.get()) {
               autoOffset(consumer);
           }
       }catch (WakeupException e){
           //if (!closed.get()) throw e;
           logger.info("捕获到WakeupException异常,进行关闭消费者处理");
       }finally {
           consumer.close();
           logger.info("终止消费者成功",consumer);
       }
    }

    /**
     * @desc 自动提交偏移量
     * @author maokeluo
     * @methodName autoOffset
     * @param  consumer
     * @create 18-1-2
     * @return void
     */
    public static void autoOffset(KafkaConsumer<String, String> consumer ){
        ConsumerRecords<String, String> records = consumer.poll(100);

        for (ConsumerRecord<String, String> record : records){
            String index = indexMap.get(record.key());
            //消费kafka中的数据发送到ES中
            Es.setData(record.value(), index, record.key());
        }
    }

    /**
     * @desc 手动提交偏移量,当数据达到一定数量时再开始消费
     * @author maokeluo
     * @methodName manualOffset
     * @param  consumer
     * @create 18-1-2
     * @return void
     */
    public static void manualOffset(KafkaConsumer<String, String> consumer){
        final int minBatchSize = 1;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                System.out.println("----------------开始接收数据-----------------");
                for (ConsumerRecord<String,String> record : buffer){
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    public static void getIndex(){
        Map<String,String> map = new HashMap<>();
        File path = new File(System.getProperty("user.dir"));
        File file = new File(path,"topicConfig.yml");
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            JSONArray jsonArray = Yaml.loadType(inputStream, JSONArray.class);
            IntStream.range(0,jsonArray.size())
                    .mapToObj(jsonArray::getJSONObject)
                    .forEach(p->{
                        String index = p.getString("index");
                        String topic = p.getString("topic");
                        map.put(topic,index);
                    });
            indexMap = map;
        } catch (FileNotFoundException e) {
            logger.error("创建文件流失败,没找到文件",e);
        }
    }


    /**
     * @desc 终止消费者
     * @author maokeluo
     * @methodName restart
     * @param
     * @create 18-1-10
     * @return void
     */
    public void shutdown(){
        closed.set(true);
        consumer.wakeup();
    }

    public void start(){
        closed.set(false);
    }
}
