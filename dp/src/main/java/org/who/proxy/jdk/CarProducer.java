package org.who.proxy.jdk;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CarProducer implements Producer {
    @Override
    public void produce() {
        log.info("benz");
        log.info("bmw");
        log.info("audi");
        log.info("...");
    }
}
