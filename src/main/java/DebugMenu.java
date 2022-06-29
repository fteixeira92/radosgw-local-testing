import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class DebugMenu {
    private static final String ADMIN_USER = "nano";
    public static final String CONFIG_LOCATION = "src/main/java/config/local_config.json";

    private static final Scanner stdIn = new Scanner(System.in);
    private static final String SPACER = "================================";
    private static final String CLEAR = "\n\n\n\n\n\n\n\n\n\n";

    public static void debugMenu() {
        try {
            System.out.println(SPACER);
            System.out.println("=          DEBUG MENU          =");
            System.out.println(SPACER);
            loginMenu(new LocalConfig(CONFIG_LOCATION));
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void loginMenu(LocalConfig config) {
        while (true) {
            System.out.println("Login as user (empty user to quit):");
            String userKey = stdIn.nextLine();
            if (userKey.equals("")) break;
            if (!userKey.equals("nano")) {
                userKey = "sbd.zhh2." + userKey;
            }
            S3Client s3Client = getS3Client(userKey, config);
            RgwAdminClient rgwAdmin = getRgwAdmin(ADMIN_USER, config);
            System.out.println(CLEAR);
            mainMenu(s3Client, rgwAdmin, config.get("object-path"));
        }
    }

    public static void mainMenu(S3Client s3Client, RgwAdminClient rgwAdminClient, String objectsPath) {
        int option;
        do {
            System.out.println("Logged in as \"" + s3Client.gerUserName() + "\"");
            System.out.println(SPACER);
            System.out.println("Choose one of the following options:");
            System.out.println("1 - Bucket operations");
            System.out.println("2 - Object operations");
            System.out.println("3 - Admin operations");
            System.out.println("4 - Bucket policies");
            System.out.println("0 - Return to login menu");
            option = getMenuOption();

            switch (option) {
                case 1:
                    menuOption(() -> bucketMenu(s3Client));
                    break;
                case 2:
                    menuOption(() -> objectMenu(s3Client, objectsPath));
                    break;
                case 3:
                    menuOption(() -> adminMenu(rgwAdminClient));
                    break;
                case 4:
                    menuOption(() -> bucketPolicyMenu(s3Client));
                    break;
                case 0:
                    break;
                default:
                    menuOptionEnter("Invalid option!");
                    break;
            }
        } while (option != 0);
        System.out.println(CLEAR);
    }

    private static void bucketMenu(S3Client s3Client) {
        int option;
        do {
            System.out.println("Choose one of the following options:");
            System.out.println("1 - Create new bucket");
            System.out.println("2 - Remove bucket");
            System.out.println("3 - List all buckets");
            System.out.println("4 - Print bucket ACL");
            System.out.println("5 - Grant permission to user to a bucket");
            System.out.println("6 - Revoke all permissions from user to a bucket");
            System.out.println("7 - Remove non empty bucket");
            System.out.println("8 - Remove all buckets");
            System.out.println("0 - Return to previous menu");
            option = getMenuOption();

            switch (option) {
                case 1:
                    menuOptionEnter("Insert bucket name:", "Bucket successfully added!", () -> {
                        String bucketName = stdIn.nextLine();
                        s3Client.createBucket(bucketName);
                    });
                    break;
                case 2:
                    menuOptionEnter("Insert bucket name:", "Bucket successfully removed!", () -> {
                        String bucketName = stdIn.nextLine();
                        s3Client.removeBucket(bucketName);
                    });
                    break;
                case 3:
                    menuOptionEnter("Bucket list:", s3Client::printBucketList);
                    break;
                case 4:
                    menuOptionEnter("Insert bucket name:", () -> {
                        String bucketName = stdIn.nextLine();
                        s3Client.printBucketACL(bucketName);
                    });
                    break;
                case 5:
                    menuOptionEnter("Insert bucket name:", "ACL successfully edited!", () -> {
                        String bucketName = stdIn.nextLine();
                        System.out.println("Insert user Id:");
                        String userId = stdIn.nextLine();
                        System.out.println("Insert permission to be given:");
                        String permission = stdIn.nextLine();
                        s3Client.grantPermission(bucketName, userId, permission);
                    });
                    break;
                case 6:
                    menuOptionEnter("Insert bucket name:", "ACL successfully edited!", () -> {
                        String bucketName = stdIn.nextLine();
                        System.out.println("Insert user Id:");
                        String userId = stdIn.nextLine();
                        System.out.println("Insert permission to be given:");
                        s3Client.revokeAllPermissions(bucketName, userId);
                    });
                    break;
                case 7:
                    menuOptionEnter("Insert bucket name:", () -> {
                        String bucketName = stdIn.nextLine();
                        s3Client.removeObjectsFromBucket(bucketName);
                        s3Client.removeBucket(bucketName);
                    });
                    break;
                case 8:
                    menuOptionEnter(s3Client::removeAllBuckets);
                    break;
                case 0:
                    break;
                default:
                    menuOptionEnter("Invalid option!");
                    break;
            }
        } while (option != 0);
    }

    private static void objectMenu(S3Client s3Client, String objectsPath) throws IOException {
        int option;
        do {
            System.out.println("Choose one of the following options:");
            System.out.println("1 - Add object to bucket");
            System.out.println("2 - Remove object from bucket");
            System.out.println("3 - List all objects from bucket");
            System.out.println("4 - List all objects from all buckets");
            System.out.println("5 - Save object to file");
            System.out.println("0 - Return to previous menu");
            option = getMenuOption();

            switch (option) {
                case 1:
                    menuOptionEnter("Insert bucket name:", "Object successfully added!", () -> {
                        String bucketName = stdIn.nextLine();
                        System.out.println("Insert object file name:");
                        String objectName = stdIn.nextLine();
                        s3Client.addObjectToBucketMeta(bucketName, new File(objectsPath + objectName));
                    });
                    break;
                case 2:
                    menuOptionEnter("Insert bucket name:", "Object successfully removed!", () -> {
                        String bucketName = stdIn.nextLine();
                        System.out.println("Insert object file name:");
                        String objectName = stdIn.nextLine();
                        s3Client.removeObjectFromBucket(bucketName, objectName);
                    });
                    break;
                case 3:
                    menuOptionEnter("Insert bucket name:", () -> {
                        String bucketName = stdIn.nextLine();
                        s3Client.printBucketObjectList(bucketName);
                    });
                    break;
                case 4:
                    menuOptionEnter(() -> s3Client.getBucketList().forEach(bucket -> s3Client.printBucketObjectList(bucket.getName())));
                    break;
                case 5:
                    menuOptionEnter("Insert target bucket name\"", "Object successfully saved!", () -> {
                        String bucketName = stdIn.nextLine();
                        System.out.println("Insert target object name:");
                        String objectName = stdIn.nextLine();
                        System.out.println("Insert destiny file name:");
                        String destinyFile = stdIn.nextLine();
                        s3Client.saveObjectToFile(bucketName, objectName, objectsPath + destinyFile);
                    });
                    break;
                case 0:
                    break;
                default:
                    menuOptionEnter("Invalid option!");
                    break;
            }
        } while (option != 0);
    }

    private static void adminMenu(RgwAdminClient rgwAdminClient) {
        int option;
        do {
            System.out.println("Choose one of the following options:");
            System.out.println("1 - Show user information");
            System.out.println("2 - List all users");
            System.out.println("3 - List user capabilities");
            System.out.println("4 - Create new user");
            System.out.println("5 - Remove user");
            System.out.println("6 - List all buckets cluster");
            System.out.println("0 - Return to previous menu");
            option = getMenuOption();

            switch (option) {
                case 1:
                    menuOptionEnter("Insert user name:", () -> {
                        String username = stdIn.nextLine();
                        System.out.println(SPACER + "\n\n");
                        rgwAdminClient.printUserInfo(username);
                    });
                    break;
                case 2:
                    menuOptionEnter("User list:", rgwAdminClient::printUserList);
                    break;
                case 3:
                    menuOptionEnter("Insert user name:", () -> {
                        String username = stdIn.nextLine();
                        System.out.println(SPACER + "\n\n");
                        rgwAdminClient.printUserCapabilities(username);
                    });
                    break;
                case 4:
                    menuOptionEnter("Insert user name:", "User successfully created!", () -> {
                        String username = stdIn.nextLine();
                        rgwAdminClient.createUser(username, rgwAdminClient.getUserParams(stdIn));
                    });
                    break;
                case 5:
                    menuOptionEnter("Insert user name:", "User successfully removed!", () -> {
                        String username = stdIn.nextLine();
                        rgwAdminClient.removeUser(username);
                    });
                    break;
                case 6:
                    menuOptionEnter(() -> {
                        rgwAdminClient.getBucketList().forEach(System.out::println);
                    });
                    break;
                case 0:
                    break;
                default:
                    menuOptionEnter("Invalid option!");
                    break;
            }
        } while (option != 0);
    }

    private static void bucketPolicyMenu(S3Client s3Client) {
        int option;
        do {
            System.out.println("Choose one of the following options:");
            System.out.println("1 - Create new bucket policy");
            System.out.println("2 - Delete bucket policy");
            System.out.println("3 - Print bucket policy");
            System.out.println("4 - Print bucket policy statements");
            System.out.println("0 - Return to previous menu");
            option = getMenuOption();

            switch (option) {
                case 1:
                    menuOptionEnter(
                            "Insert bucket name:",
                            "Bucket policy successfully created",
                            () -> {
                                String bucketName = stdIn.nextLine();
                                s3Client.setBucketPolicy(bucketName);
                            });
                    break;
                case 2:
                    menuOptionEnter(
                            "Insert bucket name:",
                            "Bucket policy successfully removed!",
                            () -> {
                                String bucketName = stdIn.nextLine();
                                s3Client.deleteBucketPolicy(bucketName);
                            });
                    break;
                case 3:
                    menuOptionEnter("Insert bucket name:", () -> {
                        String bucketName = stdIn.nextLine();
                        s3Client.printBucketPolicy(bucketName);
                    });
                    break;
                case 4:
                    menuOptionEnter("Insert bucket name:", () -> {
                        String bucketName = stdIn.nextLine();
                        s3Client.printPolicyStatements(bucketName);
                    });
                    break;
                case 0:
                    break;
                default:
                    menuOptionEnter("Invalid option!");
                    break;
            }
        } while (option != 0);
    }

    private static void menuOption(Callback cb) {
        System.out.println(SPACER + "\n\n");
        try {
            cb.callbackFunction();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        System.out.println(SPACER + "\n\n");
    }

    private static void menuOptionEnter(Callback cb) {
        System.out.println(SPACER + "\n\n");
        try {
            cb.callbackFunction();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        promptEnterKey();
        System.out.println(SPACER + "\n\n");
    }

    private static void menuOptionEnter(String message) {
        System.out.println(SPACER + "\n\n");
        System.out.println(message);
        promptEnterKey();
        System.out.println(SPACER + "\n\n");
    }

    private static void menuOptionEnter(String initialMessage, Callback cb) {
        System.out.println(SPACER + "\n\n");
        System.out.println(initialMessage);
        try {
            cb.callbackFunction();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        promptEnterKey();
        System.out.println(SPACER + "\n\n");
    }

    private static void menuOptionEnter(String initialMessage, String successMessage, Callback cb) {
        System.out.println(SPACER + "\n\n");
        System.out.println(initialMessage);
        try {
            cb.callbackFunction();
            System.out.println(SPACER);
            System.out.println(successMessage);
        } catch (Exception e) {
            System.out.println(SPACER);
            System.err.println(e.getMessage());
        }
        promptEnterKey();
        System.out.println(SPACER + "\n\n");
    }

    private static int getMenuOption() {
        try {
            return Integer.parseInt(stdIn.nextLine());
        } catch (Exception ex) {
            return -1;
        }
    }

    private static void promptEnterKey() {
        System.out.println("\nPress \"ENTER\" to return");
        stdIn.nextLine();
    }

    public static S3Client getS3Client(String userKey, LocalConfig config) {
        return new S3Client(config.get("endpoint"), config.get("region"), config.getUserKey(userKey, "access-key"),
                config.getUserKey(userKey, "secret-key"));
    }

    public static RgwAdminClient getRgwAdmin(String adminKey, LocalConfig config) {
        return new RgwAdminClient(config.get("admin-endpoint"), config.getUserKey(adminKey, "access-key"), config.getUserKey(adminKey, "secret-key"));
    }
}
