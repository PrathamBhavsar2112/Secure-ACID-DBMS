package interfaces;

public interface IAuthentication {
    void register();
    String login();
    void recoverPassword();
    void resetPassword();
}