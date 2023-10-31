package com.mapper;

import com.bean.ProjectId;

import java.util.Map;

public interface ProjectIdMapper {
    public ProjectId selectByOldProjectId(String projectOldId);
    public int addProjectId(Map<String,Object> map);
    public Integer findMaxId();
}
