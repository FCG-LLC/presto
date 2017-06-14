package co.llective.presto.hyena;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class HyenaConfig {
    private String hyenaHost = "localhost";

    public String getHyenaHost()
    {
        return hyenaHost;
    }

    @Config("hyena.url")
    @ConfigDescription("Hyena host address")
    public HyenaConfig setHyenaHost(String hyenaHost)
    {
        this.hyenaHost = hyenaHost;
        return this;
    }

}
