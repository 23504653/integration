package com.mapper;

import com.bean.ExceptHouseId;

import java.util.Map;

public interface ExceptHouseIdMapper {
    ExceptHouseId selectByOldHouseId(String OldId);
    int addExceptHouseId(Map<String,Object> map);
    Integer findMaxId();
}
