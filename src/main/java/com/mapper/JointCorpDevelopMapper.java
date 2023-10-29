package com.mapper;

import com.bean.JointCorpDevelop;

import java.util.List;
import java.util.Map;

public interface JointCorpDevelopMapper{
    public JointCorpDevelop selectByDevelopId(String developId);
    public int addJointCorpDevelopMapper(Map<String,Object> map);
    public List<JointCorpDevelop> findAll();
    public Integer findMaxId();
}
