package co.llective.presto.hyena.enrich.topdisco;

import co.llective.presto.hyena.enrich.util.IpUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TopdiscoProviderUnitTest
{
    public static class PopulateTopdiscoData
    {
        TopdiscoProvider provider;

        private static final String IP4_1 = "127.0.0.1";
        private static final String ROUTER_IP4 = "150.0.0.1";
        private static final String IP6_1 = "2001:470:1:5dd::1";
        private static final String NAME_1 = "firstName";
        private static final String ROUTER_NAME = "routerName";
        private static final String NAME_2 = "secondName";
        private static final String PORT_1 = "firstPort";
        private static final String PORT_2 = "secondPort";
        private static final int INDEX_1 = 1;
        private static final int INDEX_2 = 2;

        private void populateTestData()
        {
            TopdiscoEnrichment enrichment = new TopdiscoEnrichment(createIps(), createInterfaces());
            provider.populateTopdiscoData(enrichment);
        }

        private List<TopdiscoEnrichment.Ip> createIps()
        {
            TopdiscoEnrichment.Ip ipv4 = new TopdiscoEnrichment.Ip(IP4_1, NAME_1, (short) 2);
            TopdiscoEnrichment.Ip routerIpv4 = new TopdiscoEnrichment.Ip(ROUTER_IP4, ROUTER_NAME, (short) 0);
            TopdiscoEnrichment.Ip ipv6 = new TopdiscoEnrichment.Ip(IP6_1, NAME_2, (short) 0);

            return Arrays.asList(ipv4, routerIpv4, ipv6);
        }

        private List<TopdiscoEnrichment.Interface> createInterfaces()
        {
            TopdiscoEnrichment.Interface ipv4 = new TopdiscoEnrichment.Interface(PORT_1, INDEX_1, new HashSet<>(
                    Collections.singletonList(IP4_1)));
            TopdiscoEnrichment.Interface ipv6 = new TopdiscoEnrichment.Interface(PORT_2, INDEX_2, new HashSet<>(
                    Collections.singletonList(IP6_1)));

            return Arrays.asList(ipv4, ipv6);
        }

        private String getIpName(String ip)
        {
            IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
            return provider.getIpName(ipPair.getHighBits(), ipPair.getLowBits());
        }

        private String getRouterName(String ip)
        {
            IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
            return provider.getRouterName(ipPair.getHighBits(), ipPair.getLowBits());
        }

        private String getInterfaceName(String ip, int interfaceNo)
        {
            IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
            return provider.getInterfaceName(ipPair.getHighBits(), ipPair.getLowBits(), interfaceNo);
        }

        @BeforeMethod
        public void setUp()
        {
            provider = TopdiscoProvider.getInstance();
        }

        @Test
        public void routerNameReturnsRouterIPwhenNothingPopulated()
        {
            String routerIp = "200.200.200.200";
            assertEquals(getRouterName(routerIp), routerIp);
        }

        @Test
        public void ipNameReturnsIPwhenNothingPopulated()
        {
            String routerIp = "200.200.200.200";
            assertEquals(getIpName(routerIp), routerIp);
        }

        @Test
        public void returnsInterfaceStringWhenNothingPopulated()
        {
            String routerIp = "200.200.200.200";
            int interfaceNo = 10;
            assertEquals(getInterfaceName(routerIp, interfaceNo), Integer.toString(interfaceNo));
        }

        @Test
        public void routerNameReturnsPopulatedRouterName()
        {
            populateTestData();

            String actualName = getRouterName(ROUTER_IP4);
            assertEquals(actualName, ROUTER_NAME);
        }

        @Test
        public void routerNameReturnsIpStringWhenEntryTypeGreaterThanTwo()
        {
            populateTestData();

            String actualName = getRouterName(IP4_1);
            assertEquals(actualName, IP4_1);
        }

        @Test
        public void ipNameReturnsPopulatedIpName()
        {
            populateTestData();

            String actualName = getIpName(IP4_1);
            assertEquals(actualName, NAME_1);
        }

        @Test
        public void interfaceNameReturnsPopulatedData()
        {
            populateTestData();

            String actualName = getInterfaceName(IP4_1, INDEX_1);
            assertEquals(actualName, PORT_1);
        }
    }
}
