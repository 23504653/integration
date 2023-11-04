package houseData.beforeStartup;

import com.mapper.WorkBookMapper;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class createWorkBook0 {

    public static void main(String agr[]) throws SQLException {
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        WorkBookMapper workBookMapper = sqlSession.getMapper(WorkBookMapper.class);
        Map<String,Object> map = new HashMap<>();
        System.out.println("没有用");
    }

}
