package co.llective.presto.hyena.enrich.appname;

import java.util.LinkedHashMap;
import java.util.Objects;

public class EnrichedAppNames
{
    private LinkedHashMap<String, String> names;
    private LinkedHashMap<Integer, String> ports;

    EnrichedAppNames(LinkedHashMap<String, String> names, LinkedHashMap<Integer, String> ports)
    {
        this.names = names;
        this.ports = ports;
    }

    LinkedHashMap<String, String> getNames()
    {
        return names;
    }

    LinkedHashMap<Integer, String> getPorts()
    {
        return ports;
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
        EnrichedAppNames that = (EnrichedAppNames) o;
        return Objects.equals(names, that.names) &&
                Objects.equals(ports, that.ports);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(names, ports);
    }
}
