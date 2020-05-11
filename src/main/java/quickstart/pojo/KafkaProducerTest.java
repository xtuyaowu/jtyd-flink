package quickstart.pojo;

public class KafkaProducerTest {
    String id;
    String name;
    String sex;
    Double amount;
    String flag;
    String input_timestamp;
    String massage_timestamp;

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getInput_timestamp() {
        return input_timestamp;
    }

    public void setInput_timestamp(String input_timestamp) {
        this.input_timestamp = input_timestamp;
    }

    public String getMassage_timestamp() {
        return massage_timestamp;
    }

    public void setMassage_timestamp(String massage_timestamp) {
        this.massage_timestamp = massage_timestamp;
    }

    @Override
    public String toString() {
        return "KafkaProducerTest{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", amount=" + amount +
                ", flag='" + flag + '\'' +
                ", input_timestamp='" + input_timestamp + '\'' +
                ", massage_timestamp='" + massage_timestamp + '\'' +
                '}';
    }
}
