package com.mapper;

import com.bean.HouseId;

import java.util.Map;

public interface HouseIdMapper {
    HouseId selectByOldHouseId(String HouseOldId);
    int addHouseId(Map<String,Object> map);
    Integer findMaxId();
}
