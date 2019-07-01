package com.example.rabbitmqconsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@RestController
public class CounterController {

    Map<String, LongAdder> counter = RabbitmqConsumerApplication.counter;

    @GetMapping("/counter")
    public Map<String, Long> getCounter() {
        Map<String, Long> result = new HashMap<>();
        result.put("count1", counter.get("count1").sumThenReset());
        result.put("count5", counter.get("count5").sumThenReset());
        result.put("count10", counter.get("count10").sumThenReset());
        result.put("count", counter.get("count").sumThenReset());
        return result;
    }
}
