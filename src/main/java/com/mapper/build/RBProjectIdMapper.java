package com.mapper.build;

import com.bean.build.RBProjectId;

import java.util.Map;

public interface RBProjectIdMapper {
    public RBProjectId selectByOldProjectId(String projectOldId);
    public int addProjectId(Map<String,Object> map);
    public Integer findMaxId();
}

