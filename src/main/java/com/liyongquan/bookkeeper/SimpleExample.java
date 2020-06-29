package com.liyongquan.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

/**
 * 连续写入100条信息，然后读出
 */
public class SimpleExample {
    public static void main(String[] args) throws BKException, InterruptedException, IOException {
        // Create a client object for the local ensemble. This
        // operation throws multiple exceptions, so make sure to
        // use a try/catch block when instantiating client objects.
        BookKeeper bkc = new BookKeeper("localhost:2181");

        // A password for the new ledger
        byte[] ledgerPassword = "password".getBytes()/* some sequence of bytes, perhaps random */;

        // Create a new ledger and fetch its identifier
        LedgerHandle lh = bkc.createLedger(BookKeeper.DigestType.MAC, ledgerPassword);
        long ledgerId = lh.getId();

        // Create a buffer for four-byte entries
        ByteBuffer entry = ByteBuffer.allocate(4);

        int numberOfEntries = 100;

        // Add entries to the ledger, then close it
        for (int i = 0; i < numberOfEntries; i++) {
            entry.putInt(i);
            entry.position(0);
            lh.addEntry(entry.array());
        }
        lh.close();

        // Open the ledger for reading
        lh = bkc.openLedger(ledgerId, BookKeeper.DigestType.MAC, ledgerPassword);

        // Read all available entries
        Enumeration<LedgerEntry> entries = lh.readEntries(0, numberOfEntries - 1);

        while (entries.hasMoreElements()) {
            ByteBuffer result = ByteBuffer.wrap(entries.nextElement().getEntry());
            Integer retrEntry = result.getInt();

            // Print the integer stored in each entry
            System.out.println(String.format("Result: %s", retrEntry));
        }

        // Close the ledger and the client
        lh.close();
        bkc.close();
    }
}
