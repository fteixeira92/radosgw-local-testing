import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.actions.S3Actions;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class Demo {
    public static void main(String[] args) throws Exception {
        S3Client s3Client = DebugMenu.getS3Client("nano", new LocalConfig(DebugMenu.CONFIG_LOCATION));
        S3Client devS3Client = DebugMenu.getS3Client("sbd.dev", new LocalConfig(DebugMenu.CONFIG_LOCATION));
        RgwAdminClient rgwClient = DebugMenu.getRgwAdmin("nano", new LocalConfig(DebugMenu.CONFIG_LOCATION));

        DebugMenu.debugMenu();

        //Federated
        //Service
        //*
    }
}