package com.bean;

/***
 * 生成业务ID对应旧记录iD关系表
 * @author wxy
 */
public class BusinessId {
    private long id;
    private String oid;

    public BusinessId() {
    }

    public BusinessId(long id, String oid) {
        this.id = id;
        this.oid = oid;
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
}
