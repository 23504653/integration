package houseData;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mapper.FidCompareMapper;
import com.utils.ControlDirectory;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.ibatis.session.SqlSession;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class seaweedMain {

    private static final String BEGIN_DATE = "2023-10-12";
    //临时保存目录

    private static String DB_SEAWEED_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static final String directoryPath = "E:\\weedfsPicture";
    private static final String SEAWEED_ERROR_FILE="/SeaweedError.sql";
    private static final String seaweedFSDownloadURL = "http://192.168.1.21:9380";
    private static final String seaweedFSUploadURL = "http://192.168.1.21:9333";
    private static BufferedWriter SeaweedWriterError;
    private static File SeaweedFileError;
    private static Statement seaweedStatement;
    private static ResultSet seaweedResultSet;

    public static void main(String agr[]) throws SQLException {

        SeaweedFileError = new File(SEAWEED_ERROR_FILE);
        if (SeaweedFileError.exists()){
            SeaweedFileError.delete();
        }
        try{
            SeaweedFileError.createNewFile();
            FileWriter fw = new FileWriter(SeaweedFileError.getAbsoluteFile());
            SeaweedWriterError = new BufferedWriter(fw);
            SeaweedWriterError.write("错误记录");
            SeaweedWriterError.newLine();
            SeaweedWriterError.flush();
        }catch (IOException e){
            System.out.println("sqlWriter 文件创建失败");
            e.printStackTrace();
            return;
        }


        seaweedStatement = MyConnection.getStatement(DB_SEAWEED_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        FidCompareMapper fidCompareMapper =  sqlSession.getMapper(FidCompareMapper.class);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        Map<String,Object> map = new HashMap<>();


        try {
            seaweedResultSet = seaweedStatement.executeQuery("SELECT ID"
                    +" FROM UPLOAD_FILE"
                    +" ORDER BY UPLOAD_TIME");
            seaweedResultSet.last();
            int sumCount = seaweedResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            seaweedResultSet.beforeFirst();
            while (seaweedResultSet.next()){
//             查询表已有操作记录，没有的话才操作
               map.clear();
               if(fidCompareMapper.selectOne(seaweedResultSet.getString("ID")) == null) {
                   //每次建立图片保存目录，并清空目录
                   ControlDirectory.createDirectory(directoryPath);
                   //下载图片
                   HttpGet httpGet = new HttpGet(seaweedFSDownloadURL + "/"+ seaweedResultSet.getString("ID"));
                   HttpResponse response = httpClient.execute(httpGet);
                   if (response.getStatusLine().getStatusCode() == 200) {
                       InputStream content = response.getEntity().getContent();
                       try (FileOutputStream fos = new FileOutputStream(directoryPath+"\\"+ seaweedResultSet.getString("ID")+".jpg")) {
                           int inByte;
                           while ((inByte = content.read()) != -1) {
                               fos.write(inByte);
                           }
                       }
                       System.out.println("Image downloaded to: " + directoryPath+"\\"+ seaweedResultSet.getString("ID")+".jpg");
                   } else {
                       System.out.println("Failed to download image. HTTP status code: " + response.getStatusLine().getStatusCode()+"--:"+directoryPath+"\\"+ seaweedResultSet.getString("ID")+".jpg");
                       SeaweedWriterError.newLine();
                       SeaweedWriterError.write("ID Not Find picture--:"+seaweedResultSet.getString("ID"));
                       SeaweedWriterError.flush();
                   }
                   //上传图片
                   HttpPost httpPost = new HttpPost(seaweedFSUploadURL + "/submit");
                   File imageFile = new File(directoryPath+"\\"+ seaweedResultSet.getString("ID")+".jpg");
                   builder.addBinaryBody("file", imageFile, ContentType.APPLICATION_OCTET_STREAM, imageFile.getName());
                   httpPost.setEntity(builder.build());
                   response = httpClient.execute(httpPost);
                   HttpEntity responseEntity = response.getEntity();

                   if (responseEntity != null) {
                       String responseBody = EntityUtils.toString(responseEntity);
                       JsonParser jsonParser = new JsonParser();
                       JsonObject jsonObject = jsonParser.parse(responseBody).getAsJsonObject();
                       System.out.println("Upload response: " + responseBody);
                       String id=jsonObject.get("fid").getAsString();
                       System.out.println("Upload response: " + id);
                       // 记录图片ID
                       map.put("ID",jsonObject.get("fid").getAsString());
                       map.put("OID",seaweedResultSet.getString("ID"));
                       fidCompareMapper.addFidCompare(map);
                       sqlSession.commit();

                   }else {
                       System.out.println("Upload to image errer: " + response.getStatusLine().getStatusCode()+"--:"+directoryPath+"\\"+ seaweedResultSet.getString("ID")+".jpg");
                       SeaweedWriterError.newLine();
                       SeaweedWriterError.write("ID Not Upload picture--:"+seaweedResultSet.getString("ID"));
                       SeaweedWriterError.flush();
                   }

               }
               i++;
               System.out.println(i+"/"+String.valueOf(sumCount));
            }

        }catch (Exception e){
            System.out.println("id is errer-----id:"+seaweedResultSet.getString("id"));
            e.printStackTrace();
            return;
        }finally {
            sqlSession.close();
            if(seaweedResultSet!=null){
                seaweedResultSet.close();
            }
            if(seaweedStatement!=null){
                seaweedStatement.close();
            }
            MyConnection.closeConnection();
        }



    }
}
