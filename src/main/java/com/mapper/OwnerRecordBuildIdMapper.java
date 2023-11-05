package com.mapper;

import com.bean.OwnerRecordBuildId;

import java.util.Map;

public interface OwnerRecordBuildIdMapper {
     OwnerRecordBuildId selectByOldBuildId(String BuildIdOldId);
     int addBuildId(Map<String, Object> map);
     Integer findMaxId();
}
