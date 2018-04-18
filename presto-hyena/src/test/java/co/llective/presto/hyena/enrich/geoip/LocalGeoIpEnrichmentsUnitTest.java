package co.llective.presto.hyena.enrich.geoip;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class LocalGeoIpEnrichmentsUnitTest
{
    public static class Equals
    {
        private String subnet = "";
        private String city = "";
        private String country = "";
        private Double lon = 1.0;
        private Double lat = -1.0;

        @Test
        public void returnsFalseIfWrongNumberOfElements()
        {
            List<LocalGeoIpEnrichment> firstValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments first = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(firstValues);
            firstValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));

            List<LocalGeoIpEnrichment> secondValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments second = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(secondValues);
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));
            assertNotEquals(first, second);
        }

        @Test
        public void returnsFalseWhenElementsDoesntMatch()
        {
            List<LocalGeoIpEnrichment> firstValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments first = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(firstValues);
            firstValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));

            List<LocalGeoIpEnrichment> secondValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments second = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(secondValues);

            secondValues.add(new LocalGeoIpEnrichment(subnet + "1", city, country, lon, lat));
            assertNotEquals(first, second);

            secondValues.clear();
            secondValues.add(new LocalGeoIpEnrichment(subnet, city + "1", country, lon, lat));
            assertNotEquals(first, second);

            secondValues.clear();
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country + "1", lon, lat));
            assertNotEquals(first, second);

            secondValues.clear();
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon + 1.0, lat));
            assertNotEquals(first, second);

            secondValues.clear();
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat + 1.0));
            assertNotEquals(first, second);
        }

        @Test
        public void returnsFalseIfElementsInWrongOrder()
        {
            List<LocalGeoIpEnrichment> firstValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments first = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(firstValues);
            firstValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));
            firstValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat + 1.0));

            List<LocalGeoIpEnrichment> secondValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments second = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(secondValues);
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat + 1.0));
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));

            assertNotEquals(first, second);
        }

        @Test
        public void returnsTrueIfTheSameValues()
        {
            List<LocalGeoIpEnrichment> firstValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments first = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(firstValues);
            firstValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));

            List<LocalGeoIpEnrichment> secondValues = new ArrayList<>();
            LocalGeoIpEnrichment.LocalGeoIpEnrichments second = new LocalGeoIpEnrichment.LocalGeoIpEnrichments(secondValues);
            secondValues.add(new LocalGeoIpEnrichment(subnet, city, country, lon, lat));

            assertEquals(first, second);
        }
    }
}
