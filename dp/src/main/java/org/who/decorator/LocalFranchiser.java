package org.who.decorator;

public class LocalFranchiser extends Franchiser {
    public LocalFranchiser(CarSeller manufacturer) {
        super(manufacturer);
    }

    @Override
    int sellOil() {
        return 5;
    }

    // 本地经销商卖多点
    public int sellCar() {
        return super.sellCar() + 2;
    }
}
