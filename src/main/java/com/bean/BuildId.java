package com.bean;
/***
 * 生成楼幢ID对应旧记录iD关系表
 * @author wxy
 */
public class BuildId {
    private long id;
    private String oid;
    private String name;
    private String buildName;

    public BuildId() {

    }

    public BuildId(long id, String oid, String name,String buildName) {
        this.id = id;
        this.oid = oid;
        this.name = name;
        this.buildName = buildName;
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

    public String getBuildName() {
        return buildName;
    }

    public void setBuildName(String buildName) {
        this.buildName = buildName;
    }
}
