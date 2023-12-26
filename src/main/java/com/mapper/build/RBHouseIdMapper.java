package com.mapper.build;

import com.bean.build.HouseId;

import java.util.Map;

public interface RBHouseIdMapper {
    HouseId selectByOldHouseId(String HouseOldId);
    int addHouseId(Map<String, Object> map);
    Integer findMaxId();
}
