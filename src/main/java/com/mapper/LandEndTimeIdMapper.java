package com.mapper;

import com.bean.LandEndTimeId;

import java.util.Map;

public interface LandEndTimeIdMapper {
    LandEndTimeId selectByOldId(String landEndTimeId);
    int addLandEndTimeId(Map<String,Object> map);
    Integer findMaxId();
}
