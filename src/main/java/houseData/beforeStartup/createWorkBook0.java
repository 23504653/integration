package houseData.beforeStartup;

import com.mapper.WorkBookMapper;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 无用
 */
public class createWorkBook0 {

    public static void main(String agr[]) throws SQLException {
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        WorkBookMapper workBookMapper = sqlSession.getMapper(WorkBookMapper.class);
        Map<String,Object> map = new HashMap<>();
        //land_property,集体
//        map.put("id",1);
//        map.put("oid",);

    }

}
