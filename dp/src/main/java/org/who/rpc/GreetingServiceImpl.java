package org.who.rpc;

public class GreetingServiceImpl implements GreetingService {
    @Override
    public String greeting(String name) {
        return "hello " + name;
    }
}
