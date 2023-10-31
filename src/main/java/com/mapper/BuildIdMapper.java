package com.mapper;

import com.bean.BuildId;

import java.util.Map;

public interface BuildIdMapper {
     BuildId selectByOldBuildId(String BuildIdOldId);
     int addBuildId(Map<String,Object> map);
     Integer findMaxId();
}
