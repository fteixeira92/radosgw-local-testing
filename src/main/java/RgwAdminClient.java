import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.impl.RgwAdminException;
import org.twonote.rgwadmin4j.model.BucketInfo;
import org.twonote.rgwadmin4j.model.User;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;

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
        if (!user.getDisplayName().isEmpty()) System.out.println("\tDisplay name: " + user.getDisplayName());
        if (!user.getEmail().isEmpty()) System.out.println("\tEmail: " + user.getEmail());
        System.out.println("\tAccess key: " + user.getS3Credentials().get(0).getAccessKey());
        System.out.println("\tSecret key: " + user.getS3Credentials().get(0).getSecretKey());
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
        if (!displayName.equals("")) params.put("display-name", displayName);
        System.out.println("Insert user email:");
        String email = stdIn.nextLine();
        if (!email.equals("")) params.put("email", email);
        System.out.println("Insert user access key:");
        params.put("access-key", stdIn.nextLine());
        System.out.println("Insert user secret key:");
        params.put("secret-key", stdIn.nextLine());
        return params;
    }

    public User createUser(String userName, Map<String, String> params) {
        if (doesUserExist(userName)) return modifyUser(userName, params);
        else return rgwAdmin.createUser(userName, params);
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
}
