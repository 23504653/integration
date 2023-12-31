package houseData.beforeStartup;

import com.mapper.BizFileIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createBizFileId17 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement buildStatement;
    private static ResultSet buildResultSet;

    public static void main(String agr[]) throws SQLException {
        buildStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        BizFileIdMapper bizFileIdMapper =  sqlSession.getMapper(BizFileIdMapper.class);

        try {
            buildResultSet = buildStatement.executeQuery("select * from BUSINESS_FILE order by BUSINESS_ID");

            Map<String,Object> map = new HashMap<>();
            buildResultSet.last();


            int sumCount = buildResultSet.getRow(),i=0;
            Integer j = bizFileIdMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 2384000;//最大3638072 共 1254073  下一起始 7280144
            }

            System.out.println("记录总数-"+sumCount);
            buildResultSet.beforeFirst();
            while(buildResultSet.next()){
                map.clear();
                if(bizFileIdMapper.selectByOldId(buildResultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("id",j.intValue());
                    map.put("oid",buildResultSet.getString("ID"));

                    bizFileIdMapper.addFileId(map);
                    sqlSession.commit();
                    j++;
                }
                i++;

                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){
            System.out.println("id is errer-----id:"+buildResultSet.getString("ID"));
            e.printStackTrace();
            return;
        }finally {
            if (buildResultSet!=null){
                buildResultSet.close();
            }
            if(buildStatement!=null){
                buildStatement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }

    }
}
