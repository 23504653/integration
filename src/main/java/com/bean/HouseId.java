package com.bean;

public class HouseId {
    private long id;
    private String oid;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public HouseId() {
    }

    public HouseId(long id, String oid) {
        this.id = id;
        this.oid = oid;
    }
}
