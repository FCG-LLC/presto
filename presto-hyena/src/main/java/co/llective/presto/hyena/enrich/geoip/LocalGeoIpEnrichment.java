package co.llective.presto.hyena.enrich.geoip;

import java.util.List;
import java.util.Objects;

public class LocalGeoIpEnrichment
{
    private String subnet;
    private String city;
    private String country;
    private Double lon;
    private Double lat;

    public LocalGeoIpEnrichment(String subnet, String city, String country, Double lon, Double lat)
    {
        this.subnet = subnet;
        this.city = city;
        this.country = country;
        this.lon = lon;
        this.lat = lat;
    }

    public String getSubnet()
    {
        return subnet;
    }

    public String getCity()
    {
        return city;
    }

    public String getCountry()
    {
        return country;
    }

    public Double getLon()
    {
        return lon;
    }

    public Double getLat()
    {
        return lat;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalGeoIpEnrichment that = (LocalGeoIpEnrichment) o;
        return Objects.equals(subnet, that.subnet) &&
                Objects.equals(city, that.city) &&
                Objects.equals(country, that.country) &&
                Objects.equals(lon, that.lon) &&
                Objects.equals(lat, that.lat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(subnet, city, country, lon, lat);
    }

    public static class LocalGeoIpEnrichments
    {
        private List<LocalGeoIpEnrichment> enrichedGeoIps;

        LocalGeoIpEnrichments(List<LocalGeoIpEnrichment> enrichedGeoIps)
        {
            this.enrichedGeoIps = enrichedGeoIps;
        }

        List<LocalGeoIpEnrichment> getEnrichedGeoIps()
        {
            return enrichedGeoIps;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LocalGeoIpEnrichments that = (LocalGeoIpEnrichments) o;
            return Objects.equals(enrichedGeoIps, that.enrichedGeoIps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(enrichedGeoIps);
        }
    }
}
