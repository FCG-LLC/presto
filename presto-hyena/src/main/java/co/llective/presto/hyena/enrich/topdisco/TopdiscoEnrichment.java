package co.llective.presto.hyena.enrich.topdisco;

import java.util.List;
import java.util.Objects;
import java.util.Set;

class TopdiscoEnrichment
{
    private final List<Ip> ips;
    private final List<Interface> interfaces;

    TopdiscoEnrichment(List<Ip> ips, List<Interface> interfaces)
    {
        this.ips = ips;
        this.interfaces = interfaces;
    }

    List<Ip> getIps()
    {
        return ips;
    }

    List<Interface> getInterfaces()
    {
        return interfaces;
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
        TopdiscoEnrichment that = (TopdiscoEnrichment) o;
        return Objects.equals(ips, that.ips) &&
                Objects.equals(interfaces, that.interfaces);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ips, interfaces);
    }

    static class Ip {
        private final String ip;
        private final String name;

        /**
         * Entry types:
         * 0 = SNMP device name
         * 1 = dns name from device
         * 2 = neighbor monitoring name
         * 3 = dns record entry
         *
         * Postgres type: smallint
         */
        private final short entryType;

        Ip(String ip, String name, short entryType)
        {
            this.ip = ip;
            this.name = name;
            this.entryType = entryType;
        }

        String getIp()
        {
            return ip;
        }

        String getName()
        {
            return name;
        }

        short getEntryType()
        {
            return entryType;
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
            Ip ip1 = (Ip) o;
            return entryType == ip1.entryType &&
                    Objects.equals(ip, ip1.ip) &&
                    Objects.equals(name, ip1.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(ip, name, entryType);
        }
    }

    static class Interface {
        private final String port;
        private final int index;
        private final Set<String> ips;

        Interface(String port, int index, Set<String> ips)
        {
            this.port = port;
            this.index = index;
            this.ips = ips;
        }

        String getPort()
        {
            return port;
        }

        int getIndex()
        {
            return index;
        }

        Set<String> getIps()
        {
            return ips;
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
            Interface that = (Interface) o;
            return index == that.index &&
                    Objects.equals(port, that.port) &&
                    Objects.equals(ips, that.ips);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(port, index, ips);
        }
    }
}
