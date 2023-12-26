package com.mapper.build;

import com.bean.build.RBHouseId;

import java.util.Map;

public interface RBHouseIdMapper {
    RBHouseId selectByOldHouseId(String HouseOldId);
    int addHouseId(Map<String, Object> map);
    Integer findMaxId();
}
