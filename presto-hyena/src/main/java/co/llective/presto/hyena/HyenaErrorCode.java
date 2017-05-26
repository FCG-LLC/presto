package co.llective.presto.hyena;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum HyenaErrorCode
        implements ErrorCodeSupplier
{
    HYENA_READ_ERROR(0, EXTERNAL);

    private final ErrorCode errorCode;

    HyenaErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x9901_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
