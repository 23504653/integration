package com.bean;

public class FloorBeginEnd {
    private String  inFloorName;
    private int beginFloor;
    private int endFloor;

    public FloorBeginEnd() {
    }

    public FloorBeginEnd(String inFloorName, int beginFloor, int endFloor) {
        this.inFloorName = inFloorName;
        this.beginFloor = beginFloor;
        this.endFloor = endFloor;
    }

    public String getInFloorName() {
        return inFloorName;
    }

    public void setInFloorName(String inFloorName) {
        this.inFloorName = inFloorName;
    }

    public int getBeginFloor() {
        return beginFloor;
    }

    public void setBeginFloor(int beginFloor) {
        this.beginFloor = beginFloor;
    }

    public int getEndFloor() {
        return endFloor;
    }

    public void setEndFloor(int endFloor) {
        this.endFloor = endFloor;
    }

    @Override
    public String toString() {
        return "FloorBeginEnd{" +
                "inFloorName='" + inFloorName + '\'' +
                ", beginFloor=" + beginFloor +
                ", endFloor=" + endFloor +
                '}';
    }
}
