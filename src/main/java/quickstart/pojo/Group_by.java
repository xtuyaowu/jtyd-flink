package quickstart.pojo;

public class Group_by {
    public String group_by;

    public String getGroup_by() {
        return group_by;
    }

    public Group_by() {
    }

    public Group_by(String group_by) {
        this.group_by = group_by;
    }

    public void setGroup_by(String group_by) {
        this.group_by = group_by;
    }

    @Override
    public String toString() {
        return "Group_by{" +
                "group_by='" + group_by + '\'' +
                '}';
    }
}
