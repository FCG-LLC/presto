package co.llective.presto.hyena.enrich.rest;

public class RestClientException
        extends Exception
{
    public RestClientException()
    {
        super("Rest exception occurred");
    }

    public RestClientException(String message)
    {
        super(message);
    }

    public RestClientException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
