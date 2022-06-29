import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class LocalConfig {
    private final JSONObject config;

    public LocalConfig(String location) throws FileNotFoundException {
        this.config = getConfigJObject(location);
    }

    public String get(String key) {
        return config.getString(key);
    }

    public String getUserKey(String userName, String key) {
        JSONArray users = config.getJSONArray("users");
        String userKey = "";
        for (Object userObject : users) {
            JSONObject user = (JSONObject) userObject;
            if (user.getString("name").equals(userName)) {
                userKey = user.getString(key);
            }
        }
        return userKey;
    }

    private JSONObject getConfigJObject(String location) throws FileNotFoundException {
        BufferedReader reader = new BufferedReader(
                new FileReader(location)
        );

        JSONTokener tokener = new JSONTokener(reader);
        return new JSONObject(tokener);
    }
}
