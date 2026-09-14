package ro.midra.view.application;

public class InvalidViewRequestException extends RuntimeException {
    public InvalidViewRequestException(String message) {
        super(message);
    }
}
