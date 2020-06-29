package com.liyongquan.bookkeeper;

import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class NewApiSample {
    public static void main(String[] args) throws InterruptedException, IOException, BKException, ExecutionException {
        String zkConnectionString = "127.0.0.1:2181";
        // construct a client configuration instance
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkConnectionString);
        conf.setZkLedgersRootPath("/path/to/ledgers/root");

        // build the bookkeeper client
        BookKeeper bk = BookKeeper.newBuilder(conf).build();

        byte[] password = "some-password".getBytes();

        WriteHandle wh = bk.newCreateLedgerOp()
                .withDigestType(DigestType.CRC32)
                .withPassword(password)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .execute()          // execute the creation op
                .get();             // wait for the execution to complete


    }
}
