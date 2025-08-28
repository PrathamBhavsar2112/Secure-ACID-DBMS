package interfaces;

import java.util.List;


public interface IPersistentStorage {
    void saveUser(String username, String hashedPassword, String encryptedQuestion, String encryptedAnswer);

    void updateUserPassword(String username, String newHashedPassword, String encryptedQuestion, String encryptedAnswer);

    List<String[]> readLogs();

    String getUserPassword(String username);
    String getSecurityQuestion(String username);
    String getSecurityAnswer(String username);
    void saveLog(String timestamp, String userId, String status);
    String getLastSuccessfulLogin(String userId);
}
