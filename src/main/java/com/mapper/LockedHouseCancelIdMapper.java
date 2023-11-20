package com.mapper;

import com.bean.LockedHouseCancelId;

import java.util.Map;

public interface LockedHouseCancelIdMapper {
    LockedHouseCancelId selectByOldId(String OId);
    int addLockedHouseCancelId(Map<String,Object> map);
    Integer findMaxId();
}
