package com.bean;

public class JointCorpDevelop {
    private long corpId;
    private String developerId;
    private String name;

    public JointCorpDevelop() {

    }

    public JointCorpDevelop(long corpId, String developerId, String name) {
        this.corpId = corpId;
        this.developerId = developerId;
        this.name = name;
    }

    public long getCorpId() {
        return corpId;
    }

    public void setCorpId(long corpId) {
        this.corpId = corpId;
    }

    public String getDeveloperId() {
        return developerId;
    }

    public void setDeveloperId(String developerId) {
        this.developerId = developerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "JointCorpDevelop{" +
                "corpId=" + corpId +
                ", developerId='" + developerId + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
