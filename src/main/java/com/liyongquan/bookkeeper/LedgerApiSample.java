package com.liyongquan.bookkeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Enumeration;

/**
 * 参考地址
 * https://bookkeeper.apache.org/docs/4.10.0/api/ledger-api/
 */
@Slf4j
public class LedgerApiSample {
    public BookKeeper createBkClient() throws IOException, BKException, InterruptedException, KeeperException {
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        ZooKeeper zkClient = new ZooKeeper("127.0.0.1:2181", 5000, watchedEvent -> {
        });
        //等待zk连接成功
        while (zkClient.getState() != ZooKeeper.States.CONNECTED) {
            Thread.sleep(100);
        }
        BookKeeper bkClient = new BookKeeper(config, zkClient);
        return bkClient;
    }

    public static void main(String[] args) throws InterruptedException, BKException, IOException, KeeperException {
        LedgerApiSample sample = new LedgerApiSample();
        BookKeeper bkClient = sample.createBkClient();
        byte[] password = "some-password".getBytes();
        LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.MAC, password);
        System.out.println("create ledger");
        long entryId = handle.addEntry("Some entry data".getBytes());
        System.out.println("add entry...id:" + entryId);
        //从ledger读取信息
        Enumeration<LedgerEntry> entries =
                handle.readEntries(0, handle.getLastAddConfirmed());

        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            System.out.println("Successfully read entry " + entry.getEntryId());
        }
        //delete ledger
        long ledgerId = handle.getId();
        try {
            bkClient.deleteLedger(ledgerId);
            System.out.println("delete ledger...ledgerId:" + ledgerId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        handle.close();
        bkClient.close();
    }
}
