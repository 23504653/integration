package com.mapper;

import com.bean.OwnerRecordBuildId;
import com.bean.OwnerRecordHouseId;

import java.util.Map;

public interface OwnerRecordHouseIdMapper {
    OwnerRecordHouseId selectByOldId(String OldId);
//    OwnerRecordHouseId selectByNewId(String nw)
    int addHouseId(Map<String, Object> map);
    Integer findMaxId();
}
