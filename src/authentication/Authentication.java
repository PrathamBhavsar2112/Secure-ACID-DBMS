package authentication;

import interfaces.IAuthentication;
import interfaces.IPersistentStorage;
import logging.AuditLogger;
import storage.PersistentStorage;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import java.util.Base64;

public class Authentication implements IAuthentication {
    private final IPersistentStorage storage;
    private static final long TOKEN_EXPIRY_DURATION = 5 * 60 * 1000;
    private static final String AES_KEY = "0123456789abcdef";
    private final Scanner scanner = new Scanner(System.in);
    private final Map<String, String> resetTokens = new HashMap<>();
    private final Map<String, Long> tokenExpiry = new HashMap<>();

    private String loggedInUsername = null;

    public String getUsername() {
        return loggedInUsername;
    }
    public Authentication() {
        this.storage = new PersistentStorage();
    }

    @Override
    public void register() {
        System.out.print("Enter New Username: ");
        String username = scanner.nextLine().trim();

        // Validate username: must be unique, non-empty, and must not contain spaces
        if (username.isEmpty() || storage.getUserPassword(username) != null || username.contains(" ")) {
            System.out.println("Invalid Username. Username must be unique and should not contain spaces.");
            return;
        }

        System.out.print("Enter New Password: ");
        String password = scanner.nextLine().trim();

        if (!isValidPassword(password)) {
            System.out.println("Weak Password. Password must be at least 8 characters long and include a digit and a special character.");
            return;
        }

        System.out.print("Set a Security Question: ");
        String question = scanner.nextLine().trim();

        System.out.print("Enter Answer to Security Question: ");
        String answer = scanner.nextLine().trim();

        if (answer.isEmpty()) {
            System.out.println("Security answer cannot be empty!");
            return;
        }

        try {
            // Encrypt security question and answer before storing
            storage.saveUser(username, hashPassword(password), encryptAES(question), encryptAES(answer));
            System.out.println("User Registered Successfully!");
        } catch (Exception e) {
            System.out.println("Encryption Error: " + e.getMessage());
        }
    }
    @Override
    public String login() {
        System.out.print("Enter Username: ");
        String username = scanner.nextLine().trim();

        if (username.isEmpty()) {
            System.out.println("Username cannot be empty.");
            return null;
        }

        System.out.print("Enter Password: ");
        String password = scanner.nextLine().trim();

        String storedPassword = storage.getUserPassword(username);
        if (storedPassword == null) {
            System.out.println("User not found.");
            return null;
        }


        if (!storedPassword.equals(hashPassword(password))) {
            AuditLogger.logEvent(username, "FAILED");
            System.out.println("Invalid Credentials.");
            return null;
        }

        if (!validateCaptcha()) {
            System.out.println("Captcha failed. Login denied.");
            AuditLogger.logEvent(username, "FAILED (CAPTCHA)");
            return null;
        }

        AuditLogger.logEvent(username, "SUCCESS");

        String lastLoginTime = AuditLogger.getLastSuccessfulLogin(username);
        loggedInUsername = username;

        System.out.println("Login Successful! Last login: " + (lastLoginTime != null ? lastLoginTime : "First time login."));
        return username;
    }



    @Override
    public void recoverPassword() {
        System.out.print("Enter Your Username: ");
        String username = scanner.nextLine().trim();

        String encryptedQuestion = storage.getSecurityQuestion(username);
        if (encryptedQuestion == null) {
            System.out.println("User not found.");
            return;
        }

        try {
            System.out.println("Security Question: " + decryptAES(encryptedQuestion));

            System.out.print("Enter Your Answer: ");
            String answer = scanner.nextLine().trim();
            String encryptedAnswer = storage.getSecurityAnswer(username);

            if (encryptedAnswer == null || !decryptAES(encryptedAnswer).equalsIgnoreCase(answer)) {
                System.out.println("Incorrect security answer.");
                return;
            }

            String resetToken = generateResetToken();
            resetTokens.put(username, resetToken);
            tokenExpiry.put(username, System.currentTimeMillis() + TOKEN_EXPIRY_DURATION);
            System.out.println("Your reset token: " + resetToken);
        } catch (Exception e) {
            System.out.println("Error processing security question.");
        }
    }

    @Override
    public void resetPassword() {
        System.out.print("Enter Username: ");
        String username = scanner.nextLine().trim();

        if (storage.getUserPassword(username) == null) {
            System.out.println("User not found.");
            return;
        }

        System.out.print("Enter Reset Token: ");
        String token = scanner.nextLine().trim();

        if (!resetTokens.containsKey(username) || !resetTokens.get(username).equals(token)) {
            System.out.println("Invalid or expired token.");
            return;
        }

        if (System.currentTimeMillis() > tokenExpiry.get(username)) {
            System.out.println("Token expired. Please request a new password reset.");
            resetTokens.remove(username);
            tokenExpiry.remove(username);
            return;
        }

        String newPassword;
        while (true) {
            System.out.print("Enter New Password: ");
            newPassword = scanner.nextLine().trim();
            if (!isValidPassword(newPassword)) {
                System.out.println("Password must be at least 8 characters long, contain a digit, and a special character.");
                continue;
            }

            System.out.print("Confirm New Password: ");
            String confirmPassword = scanner.nextLine().trim();

            if (!newPassword.equals(confirmPassword)) {
                System.out.println("Passwords do not match. Try again.");
            } else {
                break;
            }
        }

        // Update only the password in storage
        storage.updateUserPassword(username, hashPassword(newPassword), storage.getSecurityQuestion(username), storage.getSecurityAnswer(username));

        resetTokens.remove(username);
        tokenExpiry.remove(username);

        System.out.println("Password reset successfully! You can now log in with your new password.");
    }

    private boolean isValidPassword(String password) {
        return password.length() >= 8 && password.matches(".*\\d.*") && password.matches(".*[!@#$%^&*()_+].*");
    }

    private boolean validateCaptcha() {
        Random rand = new Random();
        int num1 = rand.nextInt(10) + 1;
        int num2 = rand.nextInt(10) + 1;

        System.out.print("Captcha: " + num1 + " + " + num2 + " = ");
        return scanner.nextInt() == (num1 + num2);
    }

    private String hashPassword(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = md.digest(input.getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashedBytes) {
                hexString.append(String.format("%02x", b));
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 Algorithm not found.");
        }
    }

    private String encryptAES(String data) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        SecretKeySpec keySpec = new SecretKeySpec(AES_KEY.getBytes(), "AES");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        return Base64.getEncoder().encodeToString(cipher.doFinal(data.getBytes()));
    }

    private String decryptAES(String encryptedData) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        SecretKeySpec keySpec = new SecretKeySpec(AES_KEY.getBytes(), "AES");
        cipher.init(Cipher.DECRYPT_MODE, keySpec);
        return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedData)));
    }

    private String generateResetToken() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
