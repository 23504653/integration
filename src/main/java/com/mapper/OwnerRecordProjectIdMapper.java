package com.mapper;

import com.bean.OwnerRecordProjectId;

import java.util.Map;

public interface OwnerRecordProjectIdMapper {
    OwnerRecordProjectId selectByOldId(String OldId);
    int addProjectId(Map<String, Object> map);
    Integer findMaxId();
}
