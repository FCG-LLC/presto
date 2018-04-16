package co.llective.presto.hyena.enrich.geoip;

import co.llective.presto.hyena.enrich.util.SoftCache;
import co.llective.presto.hyena.enrich.util.SubnetV4;
import co.llective.presto.hyena.enrich.util.SubnetV6;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static co.llective.presto.hyena.enrich.util.IpUtil.WKP;

class GeoIpProvider
{
    private GeoIpProvider() {}

    private static final String CITY_MMDB_PATH = "GeoLite2-City.mmdb";
    private static final String COUNTRY_MMDB_PATH = "GeoLite2-Country.mmdb";
    private static final Logger LOGGER = Logger.get(GeoIpProvider.class);

    private final DatabaseReader cityReader = getDatabaseReader(CITY_MMDB_PATH);
    private final DatabaseReader countryReader = getDatabaseReader(COUNTRY_MMDB_PATH);
    private ResultProvider<String> cityProvider = (inet) ->
            cityReader.city(inet).getCity().getName();
    private ResultProvider<String> countryProvider = (inet) ->
            countryReader.country(inet).getCountry().getName();
    private ResultProvider<Double> latitudeProvider = (inet) ->
            cityReader.city(inet).getLocation().getLatitude();
    private ResultProvider<Double> longitudeProvider = (inet) ->
            cityReader.city(inet).getLocation().getLongitude();
    private SoftCache<String> cityCache = new SoftCache<>();
    private Map<SubnetV4, String> localCityV4Map = new LinkedHashMap<>();
    private Map<SubnetV6, String> localCityV6Map = new LinkedHashMap<>();
    private SoftCache<String> countryCache = new SoftCache<>();
    private Map<SubnetV4, String> localCountryV4Map = new LinkedHashMap<>();
    private Map<SubnetV6, String> localCountryV6Map = new LinkedHashMap<>();
    private SoftCache<Double> latitudeCache = new SoftCache<>();
    private Map<SubnetV4, Double> localLatitudeV4Map = new LinkedHashMap<>();
    private Map<SubnetV6, Double> localLatitudeV6Map = new LinkedHashMap<>();
    private SoftCache<Double> longitudeCache = new SoftCache<>();
    private Map<SubnetV4, Double> localLongitudeV4Map = new LinkedHashMap<>();
    private Map<SubnetV6, Double> localLongitudeV6Map = new LinkedHashMap<>();

    private static class LazyHolder
    {
        static final GeoIpProvider INSTANCE = new GeoIpProvider();
    }

    static GeoIpProvider getInstance()
    {
        return LazyHolder.INSTANCE;
    }

    String getCity(long ip1, long ip2)
    {
        return getValue(ip1, ip2, cityProvider, cityCache, localCityV4Map, localCityV6Map);
    }

    String getCountry(long ip1, long ip2)
    {
        return getValue(ip1, ip2, countryProvider, countryCache, localCountryV4Map, localCountryV6Map);
    }

    Double getLatitude(long ip1, long ip2)
    {
        return getValue(ip1, ip2, latitudeProvider, latitudeCache, localLatitudeV4Map, localLatitudeV6Map);
    }

    Double getLongitude(long ip1, long ip2)
    {
        return getValue(ip1, ip2, longitudeProvider, longitudeCache, localLongitudeV4Map, localLongitudeV6Map);
    }

    private <R> R getValue(long ip1, long ip2, ResultProvider<R> provider, SoftCache<R> cache,
            Map<SubnetV4, R> v4localProvider, Map<SubnetV6, R> v6localProvider)
    {
        // check cache
        Map<Long, R> inner = cache.getInnerMap(ip1);
        if (inner.containsKey(ip2)) {
            return inner.get(ip2);
        }

        R result = null;

        // check local subnets
        if (ip1 == WKP) {
            for (Map.Entry<SubnetV4, R> entry : v4localProvider.entrySet()) {
                SubnetV4 subnet = entry.getKey();
                if ((subnet.getMask() & ip2) == subnet.getAddress()) {
                    result = entry.getValue();
                    inner.put(ip2, result);
                    return result;
                }
            }
        }
        else {
            for (Map.Entry<SubnetV6, R> entry : v6localProvider.entrySet()) {
                SubnetV6 subnet = entry.getKey();
                if ((subnet.getMaskHighBits() & ip1) == subnet.getAddressHighBits()
                        && (subnet.getMaskLowBits() & ip2) == subnet.getAddressLowBits()) {
                    result = entry.getValue();
                    inner.put(ip2, result);
                    return result;
                }
            }
        }

        // check maxmind database
        try {
            InetAddress address = getAddressFromIps(ip1, ip2);
            if (!address.isSiteLocalAddress() && !address.isLinkLocalAddress()) {
                result = provider.apply(address);
            }
        }
        catch (GeoIp2Exception | IOException error) {
            // NOP
        }
        inner.put(ip2, result);
        return result;
    }

    private DatabaseReader getDatabaseReader(String path)
    {
        InputStream database = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        try {
            return new DatabaseReader.Builder(database).build();
        }
        catch (IOException error) {
            throw new RuntimeException(error);
        }
    }

    @FunctionalInterface
    private interface ResultProvider<R>
    {
        R apply(InetAddress inetAddress) throws GeoIp2Exception, IOException;
    }

    private InetAddress getAddressFromIps(long ip1, long ip2) throws IOException
    {
        ByteBuffer buffer;
        if (ip1 == WKP) {
            buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt((int) ip2);
        }
        else {
            buffer = ByteBuffer.allocate(Long.BYTES * 2);
            buffer.putLong(ip1);
            buffer.putLong(Long.BYTES, ip2);
        }
        return InetAddress.getByAddress(buffer.array());
    }
}
