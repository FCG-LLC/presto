package co.llective.presto.hyena.enrich.appname;

import co.llective.presto.hyena.enrich.util.SoftCache;
import co.llective.presto.hyena.enrich.util.SubnetV4;
import co.llective.presto.hyena.enrich.util.SubnetV6;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class ApplicationNameCache
{
    private static final String UNKNOWN_NAME = ""; // empty string is marking ip in cache as not named
    private static Map<SubnetV4, String> ipv4Subnets = new LinkedHashMap<>();
    private static Map<SubnetV6, String> ipv6Subnets = new LinkedHashMap<>();
    private static String[] portNames;
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationNameCache.class);
    private static final SoftCache<String> cache = new SoftCache<>();

    void populateEnrichedAppNames(EnrichedAppNames appNames)
    {

    }
}
