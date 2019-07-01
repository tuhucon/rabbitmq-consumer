package com.example.rabbitmqconsumer;

import com.google.common.primitives.Longs;
import com.rabbitmq.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@SpringBootApplication
public class RabbitmqConsumerApplication implements CommandLineRunner {

     private static Map<String, LongAdder> getCounter() {
        Map<String, LongAdder> counter = new HashMap<>();
        counter.put("count1", new LongAdder());
        counter.put("count5", new LongAdder());
        counter.put("count10", new LongAdder());
        counter.put("count", new LongAdder());
        return counter;
    }

    public static Map<String, LongAdder> counter = getCounter();


    public static void main(String[] args) {
        SpringApplication.run(RabbitmqConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        String queueName = "delay.q1";
        String exchangeName = "delay.exchange1";

        for (int i = 0; i < 3; i++) {

            Channel channel = connection.createChannel();
            channel.basicQos(10);
            channel.queueDeclare(queueName, true, false, false, null);

            Consumer consumer = new DefaultConsumer(channel) {

                @Override
                public void handleDelivery(
                        String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {

                    long value = Longs.fromByteArray(body);
                    long delta = System.nanoTime() - value;

                    if (delta < 1_000_000_000L) {
                        counter.get("count1").add(1L);
                    } else if (delta < 5_000_000_000L) {
                        counter.get("count5").add(1L);
                    } else if (delta < 10_000_000_000L) {
                        counter.get("count10").add(1L);
                    } else {
                        counter.get("count").add(1L);
                    }

                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                }
            };

            channel.basicConsume(queueName, false, consumer);
        }
    }
}
