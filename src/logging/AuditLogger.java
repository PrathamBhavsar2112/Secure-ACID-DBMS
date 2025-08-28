package logging;

import storage.PersistentStorage;
import interfaces.IPersistentStorage;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AuditLogger {
    private static final IPersistentStorage storage = new PersistentStorage();
    
    public static void logEvent(String userId, String status) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        storage.saveLog(timestamp, userId, status);
    }

    public static String getLastSuccessfulLogin(String userId) {
        return storage.getLastSuccessfulLogin(userId);
    }
}
