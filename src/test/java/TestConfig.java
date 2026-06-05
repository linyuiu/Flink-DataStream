import org.linyu.config.ConfigUtil;

public class TestConfig {
    public static void main(String[] args) {
        String string = ConfigUtil.getString("kafka.bootstrap.servers");
        System.out.println(string);
    }
}
