package cn.com.bonc.kafkaDataProcess.kafka;

import cn.com.bonc.kafkaDataProcess.util.PropertyReaderUtil;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author maokeluo
 * @desc processorSupplier工厂类
 * @create 18-1-2
 */
public class ProcessorSupplierFactory implements ProcessorSupplier {
    public static final Logger logger = LoggerFactory.getLogger(ProcessorSupplierFactory.class);
    private static Map<String,Processor> processorMap = new HashMap<>();

    public static Map<String,Processor> processor(String key) {
        PropertyReaderUtil reader = new PropertyReaderUtil();
        Map<String,String> map = reader.readPropertyFile("processor.properties");
        try {
            Processor processor = (Processor) Class.forName(map.get(key)).newInstance();
            processorMap.put(key,processor);
            return processorMap;
        } catch (ClassNotFoundException e) {
            logger.error("找不到类",e);
        } catch (IllegalAccessException e) {
            logger.error(e.getStackTrace().toString());
        } catch (InstantiationException e) {
            logger.error(e.getStackTrace().toString());
        }
        return null;
    }

    @Override
    public Processor get() {
        String processKey = "";
        Iterator<String> iterator = processorMap.keySet().iterator();
        while (iterator.hasNext()){
            processKey = iterator.next();
            Processor processor = processorMap.get(processKey);
            processorMap.remove(processKey);
            return processor;
        }
        return null;
    }
}
