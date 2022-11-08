import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.model.BucketInfo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Demo {
    public static void main(String[] args) throws Exception {
        S3Client s3Client = DebugMenu.getS3Client("nano", new LocalConfig(DebugMenu.CONFIG_LOCATION));
        S3Client devS3Client = DebugMenu.getS3Client("sbd.zhh2.dev", new LocalConfig(DebugMenu.CONFIG_LOCATION));
        RgwAdminClient rgwClient = DebugMenu.getRgwAdmin("nano", new LocalConfig(DebugMenu.CONFIG_LOCATION));
        LocalConfig localConfig = new LocalConfig(DebugMenu.CONFIG_LOCATION);
        RgwAdmin rgwAdmin = rgwAdmin = new RgwAdminBuilder()
            .accessKey(localConfig.getUserKey("nano", "access-key"))
            .secretKey(localConfig.getUserKey("nano", "secret-key"))
            .endpoint(localConfig.get("admin-endpoint"))
            .build();

        AWSCredentials credentials = new BasicAWSCredentials("GFZN9T55QQB0ZAP3MFZK", "hjwZHVPV2dgg8nETdF4ODLOSrfugqMB5z0sHJa39");
        AwsClientBuilder.EndpointConfiguration config = new AwsClientBuilder
            .EndpointConfiguration("http://127.0.0.1:8000", "eu-central-1");
        AmazonS3 s3Admin = AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(config)
            .build();

        //        System.out.println(rgwAdmin.getUserQuota("sbd.zhh2.dev").get().getMaxSizeKb());
        //        System.out.println(rgwAdmin.getUserQuota("sbd.zhh2.test").get().getMaxSizeKb());
        //        System.out.println(rgwAdmin.getUserQuota("sbd.zhh2.prod").get().getMaxSizeKb());

        //        devS3Client.createBucket("buc111");
        //        s3Admin.createBucket("buc111");
        //        s3Client.createBucket("buc12");
        //        s3Admin.createBucket("asd");

//        BucketInfo info = rgwAdmin.getBucketInfo("buc1").get();
//        System.out.println("");

        DebugMenu.debugMenu();

    }
}