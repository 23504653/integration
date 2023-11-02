package com.bean;

public class HouseUseType {
    private String designUseType;
    private int value;
    private String label;

    public HouseUseType() {
    }

    public HouseUseType(String designUseType, int value, String label) {
        this.designUseType = designUseType;
        this.value = value;
        this.label = label;
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
}
