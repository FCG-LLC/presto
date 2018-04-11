package co.llective.presto.enrich.rest;

public class RestClientException
        extends Exception
{
    public RestClientException(String message)
    {
        super(message);
    }

    public RestClientException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
