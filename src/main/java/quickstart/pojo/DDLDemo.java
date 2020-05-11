package quickstart.pojo;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/21 20:30
 * @description：DDlDemo
 * @modified By：
 * @version: 1.0
 */
public class DDLDemo {
    String name;
    Integer pv;
    Integer uv;
    String st_tart;
    String t_end;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPv() {
        return pv;
    }

    public void setPv(Integer pv) {
        this.pv = pv;
    }

    public Integer getUv() {
        return uv;
    }

    public void setUv(Integer uv) {
        this.uv = uv;
    }

    public String getSt_tart() {
        return st_tart;
    }

    public void setSt_tart(String st_tart) {
        this.st_tart = st_tart;
    }

    public String getT_end() {
        return t_end;
    }

    public void setT_end(String t_end) {
        this.t_end = t_end;
    }

    @Override
    public String toString() {
        return "DDLDemo{" +
                "name='" + name + '\'' +
                ", pv=" + pv +
                ", uv=" + uv +
                ", st_tart='" + st_tart + '\'' +
                ", t_end='" + t_end + '\'' +
                '}';
    }
}
