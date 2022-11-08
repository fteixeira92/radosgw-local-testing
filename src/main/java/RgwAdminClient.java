import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.impl.RgwAdminException;
import org.twonote.rgwadmin4j.model.BucketInfo;
import org.twonote.rgwadmin4j.model.Quota;
import org.twonote.rgwadmin4j.model.S3Credential;
import org.twonote.rgwadmin4j.model.User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;

public class RgwAdminClient {
    private final RgwAdmin rgwAdmin;

    public RgwAdminClient(String endpoint, String accessKey, String secretKey) {
        this.rgwAdmin = new RgwAdminBuilder()
            .accessKey(accessKey)
            .secretKey(secretKey)
            .endpoint(endpoint)
            .build();
    }

    public void removeUser(String userId) {
        rgwAdmin.removeUser(userId);
    }

    public void printUserInfo(String userName) {
        User user = rgwAdmin.getUserInfo(userName).orElseThrow();
        System.out.println("User \"" + userName + "\" info:");
        System.out.println("\tId: " + user.getUserId());
        if (!user.getDisplayName().isEmpty()) {
            System.out.println("\tDisplay name: " + user.getDisplayName());
        }
        if (!user.getEmail().isEmpty()) {
            System.out.println("\tEmail: " + user.getEmail());
        }
        System.out.println("\tAccess key: " + user.getS3Credentials().get(0).getAccessKey());
        System.out.println("\tSecret key: " + user.getS3Credentials().get(0).getSecretKey());
        System.out.println("\tMax-buckets: " + user.getMaxBuckets());
        System.out.println("\tQuota max size: " + rgwAdmin.getUserQuota(userName).get().getMaxSizeKb());
        if (!user.getCaps().isEmpty()) {
            System.out.println("\tCapabilities: " + user.getCaps());
        } else {
            System.out.println("\tNo admin capabilities");
        }
    }

    public User getUserInfo(String userName) {
        return rgwAdmin.getUserInfo(userName).orElseThrow();
    }

    public void printUserList() {
        rgwAdmin.listUser().forEach(user ->
            System.out.println("\t" + user)
        );
    }

    public void printUserCapabilities(String userName) {
        rgwAdmin.getUserInfo(userName).orElseThrow().getCaps().forEach(cap -> System.out.println("\t" + cap));
    }

    public Map<String, String> getUserParams(Scanner stdIn) {
        Map<String, String> params = new HashMap<>();
        System.out.println("Insert user display name:");
        String displayName = stdIn.nextLine();
        if (!displayName.equals("")) {
            params.put("display-name", displayName);
        }
        System.out.println("Insert user email:");
        String email = stdIn.nextLine();
        if (!email.equals("")) {
            params.put("email", email);
        }
        System.out.println("Insert user access key:");
        params.put("access-key", stdIn.nextLine());
        System.out.println("Insert user secret key:");
        params.put("secret-key", stdIn.nextLine());
        return params;
    }

    public User createUser(String userName, Map<String, String> params) {
        if (doesUserExist(userName)) {
            return modifyUser(userName, params);
        } else {
            return rgwAdmin.createUser(userName, params);
        }
    }

    public boolean doesUserExist(String userName) throws RgwAdminException {
        try {
            getUser(userName);
            return true;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    public User getUser(String userId) throws RgwAdminException, NoSuchElementException {
        return rgwAdmin.getUserInfo(userId).orElseThrow();
    }

    public User modifyUser(String userId, Map<String, String> userParams) {
        if (!doesUserExist(userId)) {
            throw new RgwAdminException(409, "User \"" + userId + "\" does not exist");
        }

        return rgwAdmin.modifyUser(userId, userParams);
    }

    public Map<String, String> rotateUserKey(String userId) {
        S3Credential currentKeys = getUserCredentialList(userId).get(0);
        rgwAdmin.modifyUser(userId, Map.of("generate-key", "true"));
        rgwAdmin.removeS3Credential(userId, currentKeys.getAccessKey());
        S3Credential generatedKeys = getUserCredentialList(userId).get(0);
        return Map.of("accessKey", generatedKeys.getAccessKey(), "secretKey", generatedKeys.getSecretKey());
    }

    public void resetUserKeys(String userId) {
        if (!doesUserExist(userId)) {
            throw new RgwAdminException(409, "User \"" + userId + "\" does not exist");
        }
        rgwAdmin.getUserInfo(userId).orElseThrow().getS3Credentials().forEach(s3Credential -> {
            rgwAdmin.removeS3Credential(userId, s3Credential.getAccessKey());
        });
        rgwAdmin.modifyUser(userId, Map.of("generate-key", "true"));
    }

    public List<S3Credential> getUserCredentialList(String userId) {
        Optional<User> userInfo = rgwAdmin.getUserInfo(userId);
        if (userInfo.isEmpty()) {
            throw new RgwAdminException(409, "User \"" + userId + "\" does not exist");
        }
        return userInfo.get().getS3Credentials();
    }

    public void rotate100(String userId) {
        Set<String> keySet = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            rotateUserKey(userId);
            String accessKey = rgwAdmin.getUserInfo(userId).orElseThrow().getS3Credentials().get(0).getAccessKey();
            String secretKey = rgwAdmin.getUserInfo(userId).orElseThrow().getS3Credentials().get(0).getSecretKey();
            System.out.println("USER KEY - " + accessKey);
            System.out.println("USER SECRET KEY - " + secretKey);
            keySet.add(accessKey);
        }
        System.out.println("SET SIZE " + keySet.size());
    }

    public S3Credential generateKeys(String userId) {
        return rgwAdmin.createS3Credential(userId).get(2);
    }

    public void linkBucket(String bucketName, String userId) {
        BucketInfo bucketInfo = rgwAdmin.getBucketInfo(bucketName).orElseThrow();
        rgwAdmin.linkBucket(bucketName, bucketInfo.getId(), userId);
    }

    public BucketInfo getBucketInfo(String bucketName) {
        return rgwAdmin.getBucketInfo(bucketName).get();
    }

    public List<String> getBucketList() {
        return rgwAdmin.listBucket();
    }

    public void createSubUser(String userId, String subUserId, Map<String, String> params) {
        rgwAdmin.createSubUser(userId, subUserId, params);
    }

    public void removeSubUser(String userId, String subUserId) {
        rgwAdmin.removeSubUser(userId, subUserId);
    }

    public List<String> getSubUSers(String userId) {
        return rgwAdmin.listSubUser(userId);
    }

    /**
     * QUOTAS
     */

    public void printBucketQuota(String userId) {
        Quota quota = rgwAdmin.getBucketQuota(userId).get();
        System.out.println("Bucket quota user " + userId + ":");
        System.out.println("\tEnabled: " + quota.getEnabled());
        System.out.println("\tMax objects: " + quota.getMaxObjects());
        System.out.println("\tMax size: " + quota.getMaxSizeKb());
    }

    public void printUserQuota(String userId) {
        Quota quota = rgwAdmin.getUserQuota(userId).get();
        System.out.println("User quota user " + userId + ":");
        System.out.println("\tEnabled: " + quota.getEnabled());
        System.out.println("\tMax objects: " + quota.getMaxObjects());
        System.out.println("\tMax size: " + quota.getMaxSizeKb());
    }

    public boolean doesUserQuotaExist(String userId) {
        return rgwAdmin.getUserQuota(userId).isPresent();
    }

    public Quota getUserQuota(String userId) {
        Optional<Quota> quotaOptional = rgwAdmin.getUserQuota(userId);
        if (quotaOptional.isEmpty()) {
            throw new RgwAdminException(404, "User quota is not defined");
        }
        return quotaOptional.get();
    }

    public void setUserQuota(String userId, long maxSize) {
        if (!doesUserExist(userId)) {
            throw new RgwAdminException(404, "The user \" " + userId + "\" does not exist");
        }
        rgwAdmin.setUserQuota(userId, -1, maxSize);
        rgwAdmin.setBucketQuota(userId, -1, -1);
    }

    public void calculateUsedSpace(String bucketName) {
        Optional<BucketInfo> bucketInfo = rgwAdmin.getBucketInfo(bucketName);
        System.out.println(bucketInfo.get().getUsage().getRgwMain().getSize());
        System.out.println(bucketInfo.get().getUsage().getRgwMain().getSize_utilized());
        System.out.println(bucketInfo.get().getUsage().getRgwMain().getSize_actual());
        System.out.println(bucketInfo.get().getUsage().getRgwMain().getSize_kb());
        System.out.println(bucketInfo.get().getUsage().getRgwMain().getSize_kb_utilized());
        System.out.println(bucketInfo.get().getUsage().getRgwMain().getSize_kb_actual());
    }

}
