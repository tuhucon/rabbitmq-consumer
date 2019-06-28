package com.example.rabbitmqconsumer;

import com.google.common.primitives.Longs;
import com.rabbitmq.client.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

@SpringBootApplication
public class RabbitmqConsumerApplication implements CommandLineRunner {

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

        LongAdder count1 = new LongAdder();
        LongAdder count5 = new LongAdder();
        LongAdder count10 = new LongAdder();
        LongAdder count = new LongAdder();
        LongAdder total = new LongAdder();

        for (int i = 0; i < 10; i++) {

            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);

            Consumer consumer = new DefaultConsumer(channel) {

                @Override
                public void handleDelivery(
                        String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {

                    String message = new String(body, "UTF-8");
                    // process the message

                    long value = Longs.fromByteArray(body);
                    long delta = System.nanoTime() - value;

                    if (delta < 1_000_000_000L) {
                        count1.add(1L);
                    } else if (delta < 5_000_000_000L) {
                        count5.add(1L);
                    } else if (delta < 10_000_000_000L) {
                        count10.add(1L);
                    } else {
                        count.add(1L);
                    }

                    total.add(1L);

                    if (total.sum() >= 10_000) {
                        System.out.println("count1 = " + count1.sum());
                        System.out.println("count5 = " + count5.sum());
                        System.out.println("count10 = " + count10.sum());
                        System.out.println("count > 10 = " + count.sum());
                        total.reset();
                    }

                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                }
            };

            channel.basicConsume(queueName, false, consumer);
        }
    }
}
