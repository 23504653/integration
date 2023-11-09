package com.mapper;

import com.bean.FloorBeginEnd;

import java.util.Map;

public interface FloorBeginEndMapper {

    FloorBeginEnd selectByName(String Name);
    int addFloorBeginEnd(Map<String,Object> map);
    int addFloorName(Map<String,Object> map);
}
