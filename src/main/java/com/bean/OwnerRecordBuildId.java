package com.bean;
/***
 * 生成档案库楼幢ID对应旧记录iD关系表
 * @author wxy
 */
public class OwnerRecordBuildId {
    private long id;
    private String oid;
    private String name;

    public OwnerRecordBuildId() {

    }

    public OwnerRecordBuildId(long id, String oid, String name) {
        this.id = id;
        this.oid = oid;
        this.name = name;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
