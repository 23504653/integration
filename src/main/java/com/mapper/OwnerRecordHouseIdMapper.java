package com.mapper;

import com.bean.OwnerRecordBuildId;
import com.bean.OwnerRecordHouseId;

import java.util.Map;

public interface OwnerRecordHouseIdMapper {
    OwnerRecordHouseId selectByOldId(String OldId);
    int addHouseId(Map<String, Object> map);
    Integer findMaxId();
}
