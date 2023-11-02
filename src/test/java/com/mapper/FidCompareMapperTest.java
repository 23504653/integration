package com.mapper;

import com.bean.*;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FidCompareMapperTest {
    @Test
    public void test(){
        SqlSession sqlSession = MybatisUtils.getSqlSession();
//        try{
//              Map<String,Object> map = new HashMap<>();
//              map.put("id",1);
//              map.put("oid","22");

            HouseUseTypeMapper houseUseTypeMapper= sqlSession.getMapper(HouseUseTypeMapper.class);
            JointCorpDevelopMapper jointCorpDevelopMapper = sqlSession.getMapper(JointCorpDevelopMapper.class);
//            buildIdMapper.addBuildId(map);
////            BuildId buildId =   buildIdMapper.selectByOldBuildId("22");
            List<HouseUseType> list = houseUseTypeMapper.findAll();
            List<JointCorpDevelop> list1 = jointCorpDevelopMapper.findAll();
//            System.out.println(jointCorpDevelop.toString());
//            FidCompareMapper fidCompareMapper =  sqlSession.getMapper(FidCompareMapper.class);
//            FidCompare fidCompare = fidCompareMapper.selectOne("1");
//        for(HouseUseType houseUseType:list){
//            System.out.println(houseUseType.getDesignUseType());
//        }
        HouseUseType houseUseType = houseUseTypeMapper.selectByDesignUseType("1");
        if (houseUseType!=null){
            System.out.println(houseUseType.getDesignUseType());
        }


//            System.out.println(fidCompare.toString());
//            System.out.println(fidCompare.getBusinessFileId());
//            FidCompare fidCompare2 = new FidCompare();
//            fidCompare2.setId("2");
//            fidCompare2.setBusinessFileId("BusinessFileId");
//            fidCompare2.setBusinessId("BusinessId");
//            fidCompare2.setOldFid("老ID");
//            fidCompare2.setFileName("文件名称");
//            fidCompareMapper.addFidCompare(fidCompare2);
//            sqlSession.commit();
//        }catch (Exception e){
//            throw new RuntimeException(e);
//
//        }finally {
//            sqlSession.commit();
//            sqlSession.close();
//        }



    }
}
