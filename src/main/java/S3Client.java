import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Condition;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.PolicyReaderOptions;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import com.sun.jdi.request.DuplicateRequestException;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.Collection;
import java.util.List;

public class S3Client {
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
        client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withEndpointConfiguration(config).build();
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
     * Bucket Policies
     */

    // Sets a public read policy on the bucket.
    public String getPublicReadPolicy(String bucketName) {
        Policy bucketPolicy = new Policy().withStatements(
                new Statement(Statement.Effect.Allow)
                        .withPrincipals(new Principal("arn:aws:iam:::user/sbd.dev"))
                        .withActions(S3Actions.ListBuckets)
                        .withResources(new Resource("arn:aws:s3:::" + bucketName))
        );
        Collection<Statement> statements = bucketPolicy.getStatements();
        statements.add(
                new Statement(Statement.Effect.Allow)
                        .withPrincipals(new Principal("arn:aws:iam:::user/sbd.dev"))
                        .withActions(S3Actions.DeleteBucket)
                        .withResources(new Resource("arn:aws:s3:::" + bucketName))
        );
        bucketPolicy.setStatements(statements);
        return bucketPolicy.toJson();
    }

    public void setBucketPolicy(String bucketName) {
        client.setBucketPolicy(bucketName, getPublicReadPolicy(bucketName));
    }

    public void setBucketPolicy(String bucketName, String userId, S3Actions s3Action) {
        Policy bucketPolicy;
        Statement newStatement = getAllowStatement(bucketName, userId, s3Action);

        if (client.getBucketPolicy(bucketName).getPolicyText() == null) {
            bucketPolicy = new Policy().withStatements(newStatement);
        } else {
            bucketPolicy = Policy.fromJson(client.getBucketPolicy(bucketName).getPolicyText());
            Collection<Statement> statements = bucketPolicy.getStatements();
            statements.add(newStatement);
            bucketPolicy.setStatements(statements);
        }
        client.setBucketPolicy(bucketName, bucketPolicy.toJson());
    }

    public void addPrincipalToStatement(String userId, String bucketName, S3Actions s3Action) throws Exception {
        Policy policy = Policy.fromJson(client.getBucketPolicy(bucketName).getPolicyText());
        Collection<Statement> statements = policy.getStatements();
        Statement statement = null;
        for (Statement st : statements) {
            if (st.getActions().get(0).getActionName().equals(s3Action.getActionName())) statement = st;
        }
        if (statement == null) throw new Exception("Null");
        statement.getPrincipals().add(new Principal("arn:aws:iam:::user/" + userId));
        policy.setStatements(statements);
        client.setBucketPolicy(bucketName, policy.toJson());
    }

    public void setDefaultBucketPolicy(String bucketName) {
        client.setBucketPolicy(bucketName,
                new Policy().withStatements(
                        getDefaultStatement(S3Actions.ListBuckets),
                        getDefaultStatement(S3Actions.DeleteBucket),
                        getDefaultStatement(S3Actions.ListObjects),
                        getDefaultStatement(S3Actions.DeleteObject)
                ).toJson()
        );
    }

    public Statement getAllowStatement(String bucketName, String userId, S3Actions s3Action) {
        return new Statement(Statement.Effect.Allow)
                .withPrincipals(new Principal("arn:aws:iam:::user/" + userId))
                .withActions(s3Action)
                .withResources(new Resource("arn:aws:s3:::" + bucketName));
    }

    public Statement getDefaultStatement(S3Actions s3Action) {
        return new Statement(Statement.Effect.Allow).withActions(s3Action);
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
            System.out.println("\tActions:");
            statement.getActions().forEach(action -> {
                System.out.println("\t\t" + action.getActionName());
            });
            statement.getPrincipals().forEach(principal ->
                    System.out.println("\tUser: " + principal.getId()));
            statement.getResources().forEach(resource ->
                    System.out.println("\tResource: " + resource.getId()));
            System.out.println();
        });
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

    public String gerUserName() {
        try {
            return client.getS3AccountOwner().getDisplayName();
        } catch (AmazonS3Exception ex) {
            return "Invalid user!";
        }
    }
}
