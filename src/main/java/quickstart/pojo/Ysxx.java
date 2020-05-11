package quickstart.pojo;

public class Ysxx {

    public String hsm;
    public Ysxx cs;

    public String getHsm() {
        return hsm;
    }

    public void setHsm(String hsm) {
        this.hsm = hsm;
    }

    public Ysxx getCs() {
        return cs;
    }

    public void setCs(Ysxx cs) {
        this.cs = cs;
    }

    @Override
    public String toString() {
        return "Ysxx{" +
                "hsm='" + hsm + '\'' +
                ", cs=" + cs +
                '}';
    }
}
