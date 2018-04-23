package co.llective.presto.hyena.enrich.topdisco;

import co.llective.presto.hyena.enrich.util.IpUtil;
import co.llective.presto.hyena.enrich.util.SoftCache;
import io.airlift.log.Logger;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TopdiscoProvider
{
    private static final Logger log = Logger.get(TopdiscoProvider.class);
    private static final ScheduledExecutorService SCHEDULED_THREAD = Executors.newSingleThreadScheduledExecutor();
    private static final int RELOAD_PERIOD_MIN = 15;

    private final Map<Long, String> ipv4Names = new HashMap<>();
    private final Map<Long, Map<Long, String>> ipv6Names = new HashMap<>();
    private final Map<Long, String> ipv4RouterNames = new HashMap<>();
    private final Map<Long, Map<Long, String>> ipv6RouterNames = new HashMap<>();
    private final Map<Long, Map<Integer, String>> ipv4InterfaceNames = new HashMap<>();
    private final Map<Long, Map<Long, Map<Integer, String>>> ipv6InterfaceNames = new HashMap<>();
    private final SoftCache<String> ipStrings = new SoftCache<>();
    private SoftReference<Map<Integer, String>> interfaceNames = new SoftReference<>(null);

    private TopdiscoProvider()
    {
        log.info("Scheduling topdisco enrichment data updated. Reload period " + RELOAD_PERIOD_MIN);
        SCHEDULED_THREAD.scheduleAtFixedRate(
                new TopdiscoFetcher(this),
                0,
                RELOAD_PERIOD_MIN,
                TimeUnit.MINUTES
        );
    }

    private static class LazyHolder
    {
        static final TopdiscoProvider INSTANCE = new TopdiscoProvider();
    }

    static TopdiscoProvider getInstance()
    {
        return LazyHolder.INSTANCE;
    }

    private void clear()
    {
        ipv4Names.clear();
        ipv6Names.clear();
        ipv4RouterNames.clear();
        ipv6RouterNames.clear();
        ipv4InterfaceNames.clear();
        ipv6InterfaceNames.clear();
    }


    void populateTopdiscoData(TopdiscoEnrichment deserializedResponse)
    {
        int ipsLength = deserializedResponse.getIps() == null ? 0 : deserializedResponse.getIps().size();
        int interfacesLength = deserializedResponse.getInterfaces() == null ? 0 : deserializedResponse.getInterfaces().size();
        log.info("Updating topdisco ip enrichment with: " +
                ipsLength + " ips and " + interfacesLength + " interfaces");

        clear();

        if (deserializedResponse.getIps() != null) {
            for (TopdiscoEnrichment.Ip entity : deserializedResponse.getIps()) {
                populateIpEntity(entity);
            }
        }

        if (deserializedResponse.getInterfaces() != null) {
            for (TopdiscoEnrichment.Interface entity : deserializedResponse.getInterfaces()) {
                populateInterfaceEntity(entity);
            }
        }
    }

    private void populateIpEntity(TopdiscoEnrichment.Ip entity)
    {
        IpUtil.IpPair ip = IpUtil.parseIp(entity.getIp());
        if (ip == null) {
            log.warn("Unknown ip from Topdisco ip enrichment received: " + entity.getIp());
            return;
        }
        populateIpEntityName(entity, ip);
        populateIpEntityRouterName(entity, ip);
    }

    private void populateIpEntityName(TopdiscoEnrichment.Ip entity, IpUtil.IpPair ip)
    {
        if (ip.isIp4()) {
            ipv4Names.put(ip.getLowBits(), entity.getName());
        }
        else {
            Map<Long, String> submap = ipv6Names.computeIfAbsent(ip.getHighBits(), k -> new HashMap<>());
            submap.put(ip.getLowBits(), entity.getName());
        }
    }

    private void populateIpEntityRouterName(TopdiscoEnrichment.Ip entity, IpUtil.IpPair ip)
    {
        // only entryType 0 (snmp from device table) and 1 (dns names) are taken
        if (entity.getEntryType() >= 2) return;

        if (ip.isIp4()) {
            ipv4RouterNames.put(ip.getLowBits(), entity.getName());
        }
        else {
            Map<Long, String> submap = ipv6RouterNames.computeIfAbsent(ip.getHighBits(), k -> new HashMap<>());
            submap.put(ip.getLowBits(), entity.getName());
        }
    }

    private void populateInterfaceEntity(TopdiscoEnrichment.Interface entity)
    {
        String port = entity.getPort();
        int index = entity.getIndex();
        for (String ip : entity.getIps()) {
            populateInterface(port, index, ip);
        }
    }

    private void populateInterface(String port, int index, String ip)
    {
        IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
        if (ipPair == null) {
            log.warn("Unknown ip from Topdisco ip enrichment received: " + ip);
            return;
        }

        Map<Long, Map<Integer, String>> lowBitsMap;
        if (ipPair.isIp4()) {
            lowBitsMap = ipv4InterfaceNames;
        } else {
            lowBitsMap = ipv6InterfaceNames.computeIfAbsent(ipPair.getHighBits(), k -> new HashMap<>());
        }

        Map<Integer, String> interfacesMap = lowBitsMap.computeIfAbsent(ipPair.getLowBits(), k -> new HashMap<>());
        interfacesMap.put(index, port);
    }

    private String getIpStr(long ip1, long ip2) {
        String str = ipStrings.get(ip1, ip2);
        if (str == null) {
            str = new IpUtil.IpPair(ip1, ip2).toString();
            ipStrings.put(ip1, ip2, str);
        }
        return str;
    }

    private String getInterfaceStr(int interfaceNumber) {
        Map<Integer, String> ref = interfaceNames.get();
        if (ref == null) {
            ref = new HashMap<>();
            interfaceNames = new SoftReference<>(ref);
        }
        String str = ref.get(interfaceNumber);
        if (str == null) {
            str = Integer.toString(interfaceNumber);
            ref.put(interfaceNumber, str);
        }
        return str;
    }

    String getRouterName(long ip1, long ip2)
    {
        String name;
        if (ip1 == IpUtil.WKP) {
            name = ipv4RouterNames.get(ip2);
        }
        else {
            Map<Long, String> submap = ipv6RouterNames.get(ip1);
            name = submap == null ? null : submap.get(ip2);
        }
        return name == null ? getIpStr(ip1, ip2) : name;
    }

    String getInterfaceName(long ip1, long ip2, int interfaceNo)
    {
        String name = null;
        Map<Long, Map<Integer, String>> ip4Map = ip1 == IpUtil.WKP ? ipv4InterfaceNames : ipv6InterfaceNames.get(ip1);
        if (ip4Map != null) {
            Map<Integer, String> interfacesMap = ip4Map.get(ip2);
            if (interfacesMap != null) {
                name = interfacesMap.get(interfaceNo);
            }
        }
        return name == null ? getInterfaceStr(interfaceNo) : name;
    }

    String getIpName(long ip1, long ip2)
    {
        String name;
        if (ip1 == IpUtil.WKP) {
            name = ipv4Names.get(ip2);
        }
        else {
            Map<Long, String> submap = ipv6Names.get(ip1);
            name = submap == null ? null : submap.get(ip2);
        }
        return name == null ? getIpStr(ip1, ip2) : name;
    }
}
