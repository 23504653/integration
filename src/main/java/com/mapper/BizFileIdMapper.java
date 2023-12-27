package com.mapper;

import com.bean.BizFileId;

import java.util.Map;

public interface BizFileIdMapper {
    BizFileId selectByOldId(String OldId);
    int addFileId(Map<String,Object> map);
    Integer findMaxId();
}
