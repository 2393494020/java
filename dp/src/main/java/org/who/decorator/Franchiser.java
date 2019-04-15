package org.who.decorator;

/**
 * 经销商
 */
public abstract class Franchiser extends CarSeller {
    private CarSeller seller;

    public Franchiser(CarSeller seller) {
        this.seller = seller;
    }

    // 经销商还可以卖油
    abstract int sellOil();

    public int sellCar() {
        return seller.sellCar();
    }
}
