package com.mapper.build;

import com.bean.build.ProjectId;

import java.util.Map;

public interface RBProjectIdMapper {
    public ProjectId selectByOldProjectId(String projectOldId);
    public int addProjectId(Map<String,Object> map);
    public Integer findMaxId();
}

