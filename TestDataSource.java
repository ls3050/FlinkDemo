package com.dtwave.demo;

import com.dtwave.demo.model.Student;
import com.dtwave.demo.sink.SinkToMySQL;
import com.dtwave.demo.source.SourceFromMySQL;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestDataSource {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /***************** FROM KAFKA ***********************/
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("zookeeper.connect", "localhost:2181");
//        props.put("group.id", "metric-group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "latest"); //value 反序列化
//
//        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<String>(
//                "metric",  //kafka topic
//                new SimpleStringSchema(),  // String 序列化
//                props)).setParallelism(1);
//
//        dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台


        /***************** FROM MySQL ***********************/

        DataStreamSource<Student> inputFromMySQL = env.addSource(new SourceFromMySQL());

        /***************** FROM MySQL ***********************/


        /***************** TO MySQL ***********************/

        inputFromMySQL.map(new MapFunction<Student, Student>() {

            @Override
            public Student map(Student student) throws Exception {
                student.id = student.id + 10;
                return student;
            }
        }).addSink(new SinkToMySQL());

        /***************** TO MySQL ***********************/


        env.execute("Flink add data source");

    }
}
