package quickstart.pojo;

public class Zdysxx {
    public String mbzd;
    public String yzd;

    public String getMbzd() {
        return mbzd;
    }

    public void setMbzd(String mbzd) {
        this.mbzd = mbzd;
    }

    public String getYzd() {
        return yzd;
    }

    public void setYzd(String yzd) {
        this.yzd = yzd;
    }

    public Zdysxx(String mbzd, String yzd) {
        this.mbzd = mbzd;
        this.yzd = yzd;
    }

    public Zdysxx() {
    }

    @Override
    public String toString() {
        return "Zdysxx{" +
                "mbzd='" + mbzd + '\'' +
                ", yzd='" + yzd + '\'' +
                '}';
    }
}
