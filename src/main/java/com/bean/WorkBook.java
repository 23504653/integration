package com.bean;

public class WorkBook {
    private String id;
    private String oid;
    private String value;

    public WorkBook() {
    }

    public WorkBook(String id, String oid, String value) {
        this.id = id;
        this.oid = oid;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
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
