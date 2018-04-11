package co.llective.presto.enrich.username;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

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
