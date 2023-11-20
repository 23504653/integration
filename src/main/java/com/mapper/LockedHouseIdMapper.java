package com.mapper;

import com.bean.LockedHouseId;

import java.util.Map;

public interface LockedHouseIdMapper {
    LockedHouseId selectByOldId(String OId);
    int addLockedHouseId(Map<String,Object> map);
    Integer findMaxId();
}
