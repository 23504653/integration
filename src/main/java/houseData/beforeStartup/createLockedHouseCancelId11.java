package houseData.beforeStartup;

import com.mapper.LockedHouseCancelIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createLockedHouseCancelId11 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement businessStatement;
    private static ResultSet businessResultSet;

    public static void main(String agr[]) throws SQLException {
        businessStatement = MyConnection.getStatement(DB_URL, "root", "dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        LockedHouseCancelIdMapper lockedHouseCancelIdMapper = sqlSession.getMapper(LockedHouseCancelIdMapper.class);

        try {
            businessResultSet = businessStatement.executeQuery("select * from HOUSE_OWNER_RECORD.LOCKED_HOUSE_CANCEL  order by HOUSE_CODE");

            Map<String, Object> map = new HashMap<>();
            businessResultSet.last();


            int sumCount = businessResultSet.getRow(), i = 0;
            Integer j = lockedHouseCancelIdMapper.findMaxId();
            if (j != null) {
                j = j.intValue() + 1;
            } else {
                j = 1450000; //共计8067 1460000
            }

            System.out.println("记录总数-" + sumCount);
            businessResultSet.beforeFirst();
            while (businessResultSet.next()) {
                map.clear();
                if (lockedHouseCancelIdMapper.selectByOldId(businessResultSet.getString("ID")) == null) {
                    System.out.println("j-" + j);

                    map.put("id", j.intValue());
                    map.put("oid", businessResultSet.getString("ID"));
                    lockedHouseCancelIdMapper.addLockedHouseCancelId(map);
                    sqlSession.commit();

                }
                i++;
                j++;
                System.out.println(i + "/" + String.valueOf(sumCount));
            }
        } catch (Exception e) {
            System.out.println("id is errer-----id:" + businessResultSet.getString("ID"));
            e.printStackTrace();
            return;
        } finally {
            if (businessResultSet != null) {
                businessResultSet.close();
            }
            if (businessStatement != null) {
                businessStatement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }
    }
}
