package util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import quickstart.pojo.*;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MysqlDbClient {

    private static String jdbcUrl = "jdbc:mysql://localhost:3306/mydb";
    private static String username = "root";
    private static String password = "891210";
    private static String driverName = "com.mysql.jdbc.Driver";
    private static java.sql.Connection conn;
    private static PreparedStatement ps;


    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            ps = conn.prepareStatement("select * from ysxxb where zdymxid = ?");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * execute query
     *
     * @param zdymxid
     * @return
     */
    public Ysxxb query1(String zdymxid) {


        Ysxxb ysxxb0 = new Ysxxb();

        try {
            ps.setString(1, zdymxid);
            ResultSet rs = ps.executeQuery();
            if (!rs.isClosed() && rs.next()) {
                ysxxb0.setZdymxid(rs.getString(1));
                ysxxb0.setZdymxid(rs.getString(1));
                ysxxb0.setZdysxx(rs.getString(2));
                ysxxb0.setGlgx(rs.getString(3));
                ysxxb0.setGltj(rs.getString(4));
                ysxxb0.setGroup_by(rs.getString(5));
                ysxxb0.setHaving(rs.getString(6));

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ysxxb0;

    }




    // 测试代码
    public static void main(String[] args) {
        MysqlDbClient mysqlClient = new MysqlDbClient();
        Ysxxb ysxxb = new Ysxxb();
        ysxxb = mysqlClient.query1("Model20191106003");


//        List<User> users = new ArrayList<User>();
//        User user = new User("1001", "zhangsan", "123456");
//        User user1 = new User("1002", "；lisi", "654321");
//        users.add(user);
//        users.add(user1);
//        String str = JSON.toJSONString(users);
//        System.out.println(str);



//        List<String> group_bys = new ArrayList<String>();
//        String s1 = "{\"JYLSJH.JYZH\":\"FKRWCB.JYZH\"}";
//        String s2 = "{\"JYLSJH.JYRQ\":\"FKRWCB.JYRQ\"}";
//        group_bys.add(s1);
//        group_bys.add(s2);
//        String str = JSON.toJSONString(group_bys);
//        System.out.println(str);
        //List<Group_by> group_bys = JSONArray.parseArray(ysxxb.getGroup_by(), Group_by.class);

       // String  l = "{\"id\":\"1001\",\"username\":\"zhangsan\",\"password\":\"123456\"}";
        //List<User> lists = JSON.parseArray(l, User.class);
        //User user = JSON.parseObject(l, User.class);
//        System.out.println(ysxxb.getZdysxx());
        List<String> lists = JSON.parseArray(ysxxb.getZdysxx(), String.class);
        //System.out.println(lists.toString());
        for (String s:lists
             ) {
            System.out.println(s);
        }


        //   System.out.println(lists.toString());

//        /*Group_by部分*/
//        List<String> lists = JSON.parseArray(ysxxb.getGroup_by(), String.class);
//        for (String s:lists
//             ) {
//            System.out.println(s);
//        }

    }
}