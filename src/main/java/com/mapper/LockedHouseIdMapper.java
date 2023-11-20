package com.mapper;

import java.util.Map;

public interface LockedHouseIdMapper {
    LockedHouseIdMapper selectByOldId(String OId);
    int addLockedHouseId(Map<String,Object> map);
    Integer findMaxId();
}
