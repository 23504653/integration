package com.mapper.build;
import com.bean.build.RBBuildId;

import java.util.Map;

public interface RBBuildIdMapper{
        RBBuildId selectByOldBuildId(String BuildIdOldId);
        int addBuildId(Map<String,Object> map);
        Integer findMaxId();
}
