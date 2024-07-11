package com.mycompany.app;

import java.net.InetSocketAddress;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh.Result;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        // According to the cassandra documentation you can use vectors with any data type.
        // However I have found that only bigint, int, double and float work.
        // All other types hit an exception when fetching them.

        if (true) {
            session.execute(
                    "CREATE KEYSPACE k WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
            // we can create a vector of ints
            session.execute("CREATE TABLE k.t1 (id int PRIMARY KEY, v vector<int, 2>);");
            // and insert to it
            session.execute("INSERT INTO k.t1 (id, v) VALUES (1, [42, 43])");

            // we can create a vector of smallint
            session.execute("CREATE TABLE k.t2 (id int PRIMARY KEY, v vector<smallint, 2>);");
            // and insert to it
            session.execute("INSERT INTO k.t2 (id, v) VALUES (1, [52, 53])");

            // we can create a vector of varchar
            session.execute("CREATE TABLE k.t3 (id int PRIMARY KEY, v vector<varchar, 2>);");
            // and insert to it
            session.execute("INSERT INTO k.t3 (id, v) VALUES (1, ['foo', 'bar'])");
        }

        // we can fetch the int vectors
        Row result1 = session.execute("SELECT * FROM k.t1").one();
        System.out.println(result1.getVector("v", Integer.class));

        // we can NOT fetch the smallint vectors
        // this throws an exception!
        Row result2 = session.execute("SELECT * FROM k.t2").one();
        System.out.println(result2.getVector("v", Short.class));

        // we can NOT fetch the string vectors
        // this throws an exception!
        Row result3 = session.execute("SELECT * FROM k.t3").one();
        System.out.println(result3.getObject("v"));

        session.close();
    }
}
