package co.llective.presto.hyena.enrich.geoip;

import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class GeoIpFunctions
{
    private static final String U_64 = U64Type.U_64_NAME;
    private GeoIpFunctions() {}

    private static GeoIpCache geoIp = GeoIpCache.getInstance();

    @ScalarFunction("geoip_country")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice country(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        String city = geoIp.getCountry(ip1, ip2);
        return city == null ? null : Slices.utf8Slice(city);
    }

    @ScalarFunction("geoip_city")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice city(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        String country = geoIp.getCity(ip1, ip2);
        return country == null ? null : Slices.utf8Slice(country);
    }

    @ScalarFunction("geoip_latitude")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double latitude(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        return geoIp.getLatitude(ip1, ip2);
    }

    @ScalarFunction("geoip_longitude")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double longitude(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        return geoIp.getLongitude(ip1, ip2);
    }
}
