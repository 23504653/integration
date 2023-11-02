package com.bean;

public class WorkBook {
    private long id;
    private String oid;
    private String value;

    public WorkBook() {
    }

    public WorkBook(long id, String oid, String value) {
        this.id = id;
        this.oid = oid;
        this.value = value;
    }

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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
