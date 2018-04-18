package co.llective.presto.hyena.enrich.util;

public class SubnetV4
        implements Subnet
{
    private final long address;
    private final long mask;

    public SubnetV4(String address, int maskLength)
    {
        this.mask = get32Mask(maskLength);
        this.address = this.mask & IpUtil.getLongIpV4Address(address);
    }

    public long getAddress()
    {
        return address;
    }

    public long getMask()
    {
        return mask;
    }
}
