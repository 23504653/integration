package com.bean;

import lombok.Data;
/***
 * 记录 seaweed库  下载图片重新传入新的库的生成的id
 * @author wxy
 */

@Data
public class FidCompare {


    private String id;
    private String oldFid;


    public FidCompare(){

    }
    public FidCompare(String id, String oldFid) {
        this.id = id;
        this.oldFid = oldFid;

    }



    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOldFid() {
        return oldFid;
    }

    public void setOldFid(String oldFid) {
        this.oldFid = oldFid;
    }



    @Override
    public String toString() {
        return "FidCompare{" +
                "id='" + id + '\'' +
                ", oldFid='" + oldFid + '\'' +
                '}';
    }
}
