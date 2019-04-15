package org.who.decorator;

/**
 * 总代理/代理商
 */
public class Agency extends CarSeller {
    @Override
    int sellCar() {
        return 10;
    }
}
