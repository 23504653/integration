package com.mapper;

import com.bean.JointCorpDevelop;

import java.util.Map;

public interface JointCorpDevelopMapper{
    JointCorpDevelop selectByDevelopId(String developId);
    int addJointCorpDevelopMapper(Map<String,Object> map);
}
