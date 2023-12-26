package com.mapper.build;
import com.bean.build.BuildId;

import java.util.Map;

public interface RBBuildIdMapper{
        BuildId selectByOldBuildId(String BuildIdOldId);
        int addBuildId(Map<String,Object> map);
        Integer findMaxId();
}
