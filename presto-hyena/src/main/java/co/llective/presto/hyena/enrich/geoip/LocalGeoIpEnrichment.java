package co.llective.presto.hyena.enrich.geoip;

import java.util.List;

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

    public static class LocalGeoIpEnrichemnts
    {
        private List<LocalGeoIpEnrichment> enrichedGeoIps;

        LocalGeoIpEnrichemnts(List<LocalGeoIpEnrichment> enrichedGeoIps)
        {
            this.enrichedGeoIps = enrichedGeoIps;
        }

        List<LocalGeoIpEnrichment> getEnrichedGeoIps()
        {
            return enrichedGeoIps;
        }
    }
}
