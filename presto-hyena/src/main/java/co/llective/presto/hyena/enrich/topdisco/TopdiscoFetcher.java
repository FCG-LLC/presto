package co.llective.presto.hyena.enrich.topdisco;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TopdiscoFetcher
    implements Runnable
{
    private static final Logger log = Logger.get(TopdiscoFetcher.class);
    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/netdisco";
    private static final String JDBC_USERNAME = "netdisco";
    private static final String JDBC_PASSWORD = "netdisco_passw0rd";
    private static final String SQL = "SELECT row_to_json(r)\n" +
            "FROM (WITH ips AS\n" +
            "        (SELECT array_to_json(array_agg(t)) AS col\n" +
            "         FROM\n" +
            "           (SELECT DISTINCT ON (ine.ip) ine.ip,\n" +
            "                                        ine.name,\n" +
            "                                        ine.entry_type AS \"entryType\"\n" +
            "            FROM public.ip_name_enrichment ine\n" +
            "            ORDER BY ine.ip,\n" +
            "                     ine.entry_type,\n" +
            "                     ine.ts DESC) t) ,\n" +
            "           interfaces AS\n" +
            "        (SELECT array_to_json(array_agg(t)) AS col\n" +
            "         FROM\n" +
            "           (SELECT dp.port,\n" +
            "                   dp.if_index AS INDEX,\n" +
            "                   array_to_json(array_remove(array_cat(array_agg(dp.ip), array_agg(di.alias)), NULL)) AS \"ips\"\n" +
            "            FROM device_port dp\n" +
            "            LEFT JOIN device_ip di ON di.ip = dp.ip OR di.alias = dp.ip\n" +
            "            GROUP BY dp.port,\n" +
            "                     dp.if_index) t)\n" +
            "      SELECT ips.col AS \"ips\",\n" +
            "             interfaces.col AS \"interfaces\"\n" +
            "      FROM ips,\n" +
            "           interfaces) r";
    private final TopdiscoProvider topdiscoProvider;
    private int lastResponseHash;

    static {
        try {
            // load Postgres JDBC driver in runtime
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    TopdiscoFetcher(TopdiscoProvider topdiscoProvider)
    {
        this.topdiscoProvider = topdiscoProvider;
    }

    @Override
    public void run()
    {
        try {
            log.info("Reloading Topdisco ip enrichment cache");
            String response = fetchData();
            if (response == null) {
                log.warn("Cannot reload Topdisco ip enrichment - no response from Postgres received");
                return;
            }
            if (response.hashCode() == lastResponseHash) {
                log.info("No changes in Topdisco ip enrichment found");
                return;
            }
            TopdiscoEnrichment deserializedResponse = parseData(response);
            if (deserializedResponse != null) {
                lastResponseHash = response.hashCode();
                int ipsLength = deserializedResponse.getIps() == null ? 0 : deserializedResponse.getIps().size();
                int interfacesLength = deserializedResponse.getInterfaces() == null ? 0 : deserializedResponse.getInterfaces().size();
                log.info("Topdisco ip enrichment updated: " +
                        ipsLength + " ips and " + interfacesLength + " interfaces");
                topdiscoProvider.populateTopdiscoData(deserializedResponse);
            }
        } catch (Exception exc) {
            log.error("An exception occurred during Topdisco ip enrichment cache reload", exc);
        }
    }

    private String fetchData() throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(SQL)) {
                    return resultSet.next() ? resultSet.getString(1) : null;
                }
            }
        }
    }

    private TopdiscoEnrichment parseData(String json) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return objectMapper.readValue(json, TopdiscoEnrichment.class);
        } catch (IOException exc) {
            log.error("Cannot parse Topdisco ip enrichment data from Postgres", exc);
            return null;
        }
    }
}
