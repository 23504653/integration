package com.bean;

public class HouseUseType {
    private String designUseType;
    private int value;
    private String label;
    private String houseType;

    public HouseUseType() {
    }

    public HouseUseType(String designUseType, int value, String label,String houseType) {
        this.designUseType = designUseType;
        this.value = value;
        this.label = label;
        this.houseType = houseType;
    }

    public String getDesignUseType() {
        return designUseType;
    }

    public void setDesignUseType(String designUseType) {
        this.designUseType = designUseType;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getHouseType() {
        return houseType;
    }

    public void setHouseType(String houseType) {
        this.houseType = houseType;
    }
}
