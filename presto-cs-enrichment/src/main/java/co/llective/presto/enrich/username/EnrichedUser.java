package co.llective.presto.enrich.username;

import co.llective.presto.enrich.util.IpUtil;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Objects;

class EnrichedUser
{
    public EnrichedUser(Long startTs, Long endTs, IpUtil.IpPair ip, String user)
    {
        this.startTs = startTs;
        this.endTs = endTs;
        this.ip = ip;
        this.user = user;
    }

    private Long startTs;
    private Long endTs;
    private IpUtil.IpPair ip;
    private String user;

    public Long getStartTs()
    {
        return startTs;
    }

    public Long getEndTs()
    {
        return endTs;
    }

    public IpUtil.IpPair getIp()
    {
        return ip;
    }

    public String getUser()
    {
        return user;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        EnrichedUser that = (EnrichedUser) o;
        return Objects.equals(startTs, that.startTs) &&
                Objects.equals(endTs, that.endTs) &&
                Objects.equals(ip, that.ip) &&
                Objects.equals(user, that.user);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(startTs, endTs, ip, user);
    }

    @JsonDeserialize(using = EnrichedUsersDeserializer.class)
    public static class EnrichedUsers
    {
        List<EnrichedUser> enrichedUsers;

        public EnrichedUsers(List<EnrichedUser> enrichedUsers)
        {
            this.enrichedUsers = enrichedUsers;
        }

        public List<EnrichedUser> getEnrichedUsers()
        {
            return enrichedUsers;
        }
    }
}
