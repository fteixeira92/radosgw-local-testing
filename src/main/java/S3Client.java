import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Action;
import com.amazonaws.auth.policy.Condition;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.PolicyReaderOptions;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import com.sun.jdi.request.DuplicateRequestException;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class S3Client {
    private static final String PROVIDER = "AWS";
    private static final String READ_STATEMENT = "read";
    private static final String WRITE_STATEMENT = "write";
    private static final String CONDITION_IP_TYPE = "IpAddress";
    private final AmazonS3 client;

    public S3Client(String endpoint, String accessKey, String secretKey) {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        client = new AmazonS3Client(credentials, clientConfig);
        client.setEndpoint(endpoint);
    }

    public S3Client(String endpoint, String region, String accessKey, String secretKey) {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AwsClientBuilder.EndpointConfiguration config = new AwsClientBuilder.EndpointConfiguration(endpoint, region);
        client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(config)
            .build();
    }

    /**
     * Bucket Operations
     */

    public Bucket createBucket(String bucketName) throws DuplicateRequestException {
        Bucket bucket;
        if (client.doesBucketExistV2(bucketName)) {
            throw new DuplicateRequestException("Bucket \"" + bucketName + "\" already exists!");
        } else {
            bucket = client.createBucket(bucketName);
            return bucket;
        }
    }

    public boolean doesBucketExist(String bucketName) {
        return client.doesBucketExistV2(bucketName);
    }

    public Bucket getBucket(String bucket_name) {
        return client.listBuckets().stream().filter(bucket -> bucket.getName().equals(bucket_name)).findAny().orElseThrow();
    }

    public void removeBucket(String bucketName) throws AmazonS3Exception {
        client.deleteBucket(bucketName);
    }

    public List<Bucket> getBucketList() {
        return client.listBuckets();
    }

    public void printBucketList() {
        List<Bucket> buckets = getBucketList();
        buckets.forEach((b) -> System.out.println("\t" + b));
    }

    private void removeAllVersionsBucket(String bucketName) {
        VersionListing version_listing = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
        while (true) {
            for (S3VersionSummary vs : version_listing.getVersionSummaries()) {
                client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
            }
            if (version_listing.isTruncated()) {
                version_listing = client.listNextBatchOfVersions(version_listing);
            } else {
                break;
            }
        }
    }

    public void removeObjectsFromBucket(String bucketName) {
        ObjectListing object_listing = client.listObjects(bucketName);
        while (true) {
            for (S3ObjectSummary summary : object_listing.getObjectSummaries()) {
                client.deleteObject(bucketName, summary.getKey());
            }
            if (object_listing.isTruncated()) {
                object_listing = client.listNextBatchOfObjects(object_listing);
            } else {
                break;
            }
        }
    }

    //Bucket lifecycle configuration
    public void lifecycle(){
        String bucketName = "buc10";

        // Create a rule to archive objects with the "glacierobjects/" prefix to Glacier immediately.
        BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
            .withId("Archive immediately rule")
//            .withExpirationDate(new Date(System.currentTimeMillis()))
            .withStatus(BucketLifecycleConfiguration.ENABLED);

        BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
            .withRules(Arrays.asList(rule1));

        client.setBucketLifecycleConfiguration(bucketName, configuration);
    }

    public void setLifecycleConfiguration(String bucketName, int year, int month, int day, int hour, int minute) {
        BucketLifecycleConfiguration.Rule lifecycleRule = new BucketLifecycleConfiguration.Rule();
//        lifecycleRule.setExpirationDate(new Date(year, month, day, hour, minute));
        lifecycleRule.setExpirationDate(new Date(year, month, day));
        client.setBucketLifecycleConfiguration(bucketName, new BucketLifecycleConfiguration().withRules(lifecycleRule));
    }

    public void setLifecycleConfiguration(String bucketName) {
        BucketLifecycleConfiguration.Rule lifecycleRule = new BucketLifecycleConfiguration.Rule();
//        lifecycleRule.setExpirationDate(new Date(year, month, day, hour, minute));
        lifecycleRule.setExpirationDate(new Date(System.currentTimeMillis()));
        client.setBucketLifecycleConfiguration(bucketName, new BucketLifecycleConfiguration().withRules(lifecycleRule));
    }

    public BucketLifecycleConfiguration getLifecycleConfiguration(String bucketName) {
        return client.getBucketLifecycleConfiguration(bucketName);
    }

    public void printLifecycleConfiguration(String bucketName) {
        BucketLifecycleConfiguration config = getLifecycleConfiguration(bucketName);
        config.getRules().forEach(rule -> {
            System.out.println("Expiration date: " + rule.getExpirationDate());
        });
    }

    public void removeAllBuckets() {
        getBucketList().forEach(bucket -> removeBucket(bucket.getName()));
    }

    /**
     * Object Operations
     */

    public PutObjectResult addObjectToBucket(String bucketName, File object) throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(FileUtils.readFileToByteArray(object));
        return client.putObject(new PutObjectRequest(bucketName, object.getName(), input, new ObjectMetadata()));
    }

    public PutObjectResult addObjectToBucketMeta(String bucketName, File object) throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(FileUtils.readFileToByteArray(object));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(object.length());
        return client.putObject(new PutObjectRequest(bucketName, object.getName(), input, metadata));
    }

    public S3Object getObjectFromBucket(String bucketName, String objectName) {
        return client.getObject(bucketName, objectName);
    }

    public void removeObjectFromBucket(String bucketName, String objectKey) {
        client.deleteObject(bucketName, objectKey);
    }

    public void saveObjectToFile(String bucketName, String objectKey, String destinyPath) {
        client.getObject(new GetObjectRequest(bucketName, objectKey), new File(destinyPath));
    }

    public ObjectListing getBucketObjectListing(String bucketName) {
        return client.listObjects(bucketName);
    }

    public void printBucketObjectList(String bucketName) {
        ObjectListing objects = client.listObjects(bucketName);
        System.out.println("Bucket \"" + bucketName + "\" object list:");
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println("\t\t"
                    + objectSummary.getKey()
                    + " " + objectSummary.getSize()
                    + " " + StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = client.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }

    /**
     * ACL Operations
     */

    public void printBucketACL(String bucketName) {
        try {
            AccessControlList acl = client.getBucketAcl(bucketName);
            System.out.println("Bucket \"" + bucketName + "\" permissions:");
            acl.getGrantsAsList().forEach(grant -> {
                System.out.println("\t" + grant.getGrantee().getIdentifier() + ": " + grant.getPermission());
            });
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
    }

    public void printObjectACL(String bucketName, String objectName) {
        try {
            AccessControlList acl = client.getObjectAcl(bucketName, objectName);
            System.out.println("Object \"" + bucketName + "/" + objectName + "\" permissions:");
            acl.getGrantsAsList().forEach(grant -> {
                System.out.println("\t" + grant.getGrantee().getIdentifier() + ": " + grant.getPermission());
            });
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
    }

    public void grantPermission(String bucketName, String userId, String permissionName) {
        try {
            AccessControlList acl = client.getBucketAcl(bucketName);
            CanonicalGrantee grantee = new CanonicalGrantee(userId);
            Permission permission = Permission.valueOf(permissionName);
            acl.grantPermission(grantee, permission);
            client.setBucketAcl(bucketName, acl);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
    }

    public void revokeAllPermissions(String bucketName, String userId) {
        try {
            AccessControlList acl = client.getBucketAcl(bucketName);
            CanonicalGrantee grantee = new CanonicalGrantee(userId);
            acl.revokeAllPermissions(grantee);
            client.setBucketAcl(bucketName, acl);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
    }

    public String getUsername() {
        try {
            return client.getS3AccountOwner().getDisplayName();
        } catch (AmazonS3Exception ex) {
            return "Invalid user!";
        }
    }

    /**
     * Bucket Policies
     */

    public String getPolicyBucketId(String bucketName) {
        return "arn:aws:s3:::" + bucketName;
    }

    public String generatePrincipalId(String userId) {
        return "arn:aws:iam:::user/" + userId;
    }

    public String getPolicyId(String bucketName) {
        return bucketName + "%%%policy";
    }

    public String getStatementId(String userId, String statementType) {
        return userId + "%%%" + statementType;
    }

    public void deleteBucketPolicy(String bucketName) {
        client.deleteBucketPolicy(bucketName);
    }

    public void printBucketPolicy(String bucketName) {
        BucketPolicy policy = client.getBucketPolicy(bucketName);
        System.out.println(policy.getPolicyText());
    }

    public void printPolicyStatements(String bucketName) {
        Policy bucketPolicy = Policy.fromJson(client.getBucketPolicy(bucketName).getPolicyText(),
            new PolicyReaderOptions().withStripAwsPrincipalIdHyphensEnabled(false));

        System.out.println("Policy: " + bucketPolicy.getId());

        bucketPolicy.getStatements().forEach(statement -> {
            System.out.println("\tStatement: " + statement.getId());
            System.out.println("\tEffect: " + statement.getEffect());
            System.out.println("\tActions:");
            statement.getActions().forEach(action -> {
                System.out.println("\t\t" + action.getActionName());
            });

            statement.getPrincipals().forEach(principal ->
                System.out.println("\tUser: " + principal.getId()));

            statement.getResources().forEach(resource ->
                System.out.println("\tResource: " + resource.getId()));

            System.out.println("\tConditions:");
            statement.getConditions().forEach(condition -> {
                System.out.println("\t\tCondition type: " + condition.getType());
                System.out.println("\t\t\tKey: " + condition.getConditionKey());
                condition.getValues().forEach((
                    value -> {
                        System.out.println("\t\t\tValue: " + value);
                    }));

            });
            System.out.println();
        });
    }

    /**
     * POLICIES
     */

    public void setBucketPolicy(String bucketName, String bucketPolicy) {
        client.setBucketPolicy(bucketName, bucketPolicy);
    }

    public Policy getBucketPolicy(String bucketName) {
        if (!doesBucketExist(bucketName)) {
            throw new AmazonS3Exception("Unable to retrieve policy, bucket \"" + bucketName + "\" doesn't exist");
        }
        BucketPolicy bucketPolicy = client.getBucketPolicy(bucketName);
        if (bucketPolicy.getPolicyText() == null) {
            return new Policy(getPolicyId(bucketName));
        }
        return Policy.fromJson(bucketPolicy.getPolicyText(),
            new PolicyReaderOptions().withStripAwsPrincipalIdHyphensEnabled(false));
    }

    public boolean updateBucketPolicy(String bucketName, String userId, String allowedIps,
        List<Action> actions, String statementType) {
        Policy bucketPolicy = getBucketPolicy(bucketName);
        Collection<Statement> policyStatements = bucketPolicy.getStatements();
        String newStatementId = getStatementId(userId, statementType);
        boolean duplicate = policyStatements.stream().anyMatch(statement -> statement.getId().equals(newStatementId));
        if (!duplicate) {
            Statement newStatement = generateStatement(bucketName, userId, allowedIps, actions, statementType);
            policyStatements.add(newStatement);
            bucketPolicy.setStatements(policyStatements);
            client.setBucketPolicy(bucketName, bucketPolicy.toJson());
        }
        return !duplicate;
    }

    public boolean disableUserPolicyControl(String bucketName, String userId) {
        Policy bucketPolicy = getBucketPolicy(bucketName);
        Collection<Statement> policyStatements = bucketPolicy.getStatements();
        boolean duplicate = policyStatements.stream().anyMatch(statement -> statement.getId().equals("ACLs disabled"));
        if (!duplicate) {
            Statement newStatement = getDenyPolicyControlStatement(bucketName, userId);
            policyStatements.add(newStatement);
            bucketPolicy.setStatements(policyStatements);
            client.setBucketPolicy(bucketName, bucketPolicy.toJson());
        }
        return !duplicate;
    }

    private Statement getDenyPolicyControlStatement(String bucketName, String userId) {
        Statement newStatement = new Statement(Statement.Effect.Deny)
            .withPrincipals(new Principal(PROVIDER, generatePrincipalId(userId), false))
            .withActions(
                S3Actions.SetObjectAcl,
                S3Actions.GetObjectAcl,
                S3Actions.SetBucketAcl,
                S3Actions.GetBucketPolicy,
                S3Actions.DeleteBucketPolicy,
                S3Actions.SetBucketPolicy
            )
            .withResources(
                new Resource(getPolicyBucketId(bucketName)),
                new Resource(getPolicyBucketId(bucketName) + "/*")
            );
        newStatement.setId("ACLs disabled");
        return newStatement;
    }

    public Condition getBucketOwnerEnforcedCondition() {
        return new Condition()
            .withType("StringNotEquals")
            .withConditionKey("s3:x-amz-object-ownership")
            .withValues("BucketOwnerEnforced");
    }

    /**
     * Generates policy statement composed of the given parameters
     *
     * @param bucketName    bucket that the statement will be applied to
     * @param userId        statement's principal that the given conditions will be applied to
     * @param allowedIps    range of ips the statement's principal will be allowed to access from
     * @param actions       list of actions that the statement's principal will be allowed to perform
     * @param statementType type of the statement ('read'/'write')
     * @return generated policy statement
     */
    private Statement generateStatement(String bucketName, String userId, String allowedIps,
        List<Action> actions, String statementType) {
        Statement newStatement = new Statement(Statement.Effect.Allow)
            .withPrincipals(new Principal(PROVIDER, generatePrincipalId(userId), false))
            .withConditions(generateAllowedIpsCondition(allowedIps))
            .withResources(
                new Resource(getPolicyBucketId(bucketName)),
                new Resource(getPolicyBucketId(bucketName) + "/*")
            );
        newStatement.setActions(actions);
        newStatement.setId(getStatementId(userId, statementType));
        return newStatement;
    }

    /**
     * Generates policy condition that will allow access from a given rage of ips
     *
     * @param allowedIps range of ips that access will be granted from
     * @return generated policy condition
     */
    public Condition generateAllowedIpsCondition(String allowedIps) {
        return new Condition()
            .withType(CONDITION_IP_TYPE)
            .withConditionKey("aws:SourceIp")
            .withValues(Arrays.asList(allowedIps.split(",")));
    }

    public void applyReadPolicies(String bucketName, String clientId, String allowedIps, S3Client s3Client) {
        List<Action> actions = List.of(
            S3Actions.ListBuckets,
            S3Actions.ListObjects,
            S3Actions.ListObjectVersions,
            S3Actions.GetObject,
            S3Actions.GetObjectVersion
        );
        try {
            if (s3Client.updateBucketPolicy(bucketName, clientId, allowedIps, actions, READ_STATEMENT)) {
                System.out.println("Error applying policy bucket: " + bucketName + ", client: " + clientId);
            }
        } catch (AmazonS3Exception e) {
            System.out.println("Error applying policy bucket: " + bucketName + ", client: " + clientId);
        }
    }

    private void applyWritePolicies(String bucketName, String clientId, String allowedIps, S3Client s3Client) {
        List<Action> actions = List.of(
            S3Actions.CreateBucket,
            S3Actions.DeleteBucket,
            S3Actions.PutObject,
            S3Actions.DeleteObject,
            S3Actions.DeleteObjectVersion
        );
        try {
            if (s3Client.updateBucketPolicy(bucketName, clientId, allowedIps, actions, WRITE_STATEMENT)) {
                System.out.println("Error applying policy bucket: " + bucketName + ", client: " + clientId);
            }
        } catch (AmazonS3Exception e) {
            System.out.println("Error applying policy bucket: " + bucketName + ", client: " + clientId);
        }
    }
}
