package ro.midra.view.adapter.in;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ro.midra.view.application.InvalidViewRequestException;
import ro.midra.view.application.ViewAcceptanceException;

@RestControllerAdvice
public class ViewExceptionHandler {
    @ExceptionHandler(InvalidViewRequestException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse invalidViewRequest(InvalidViewRequestException error) {
        return new ErrorResponse(error.getMessage());
    }

    @ExceptionHandler(ViewAcceptanceException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public ErrorResponse viewAcceptanceFailed(ViewAcceptanceException error) {
        return new ErrorResponse(error.getMessage());
    }

    public record ErrorResponse(String message) {
    }
}
