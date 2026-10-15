/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

// file: ExampleDatabasePut.java

package je.gettingStarted;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;

// ExampleDatabasePut 是 Berkeley DB Java Edition (JE) 官方入门教程（Getting Started）中的核心示例程序。
// 主要核心作用体现在：
// 演示数据加载与写入（Put 操作）：展示如何从纯文本文件（如供应商信息文件、库存信息文件）中读取原始数据，并将其解析、转换为对应的业务对象（如 Vendor、Inventory）。
// 演示序列化与绑定机制：演示如何使用 SerialBinding 和自定义的 TupleBinding（如 InventoryBinding）将 Java 对象打包成 DatabaseEntry 写入数据库。
// 演示事务控制与错误处理：展示如何显式开启事务（beginTransaction）、在写入失败时进行事务回滚（txn.abort()）、在成功时提交事务（txn.commit()），以及如何安全地关闭数据库环境。

public class ExampleDatabasePut {
    // 指定 Berkeley DB 环境（Environment）文件存储的默认本地目录路径。
    private static File myDbEnvPath = new File("/tmp/JEDB");
    // 指定库存数据源纯文本文件的默认路径。
    private static File inventoryFile = new File("./inventory.txt");
    // 指定供应商数据源纯文本文件的默认路径。
    private static File vendorsFile = new File("./vendors.txt");

    // DatabaseEntries used for loading records
    // 用于加载记录时的 Key（键）容器（复用同一个 DatabaseEntry 实例以提高性能）。
    private static DatabaseEntry theKey = new DatabaseEntry();
    // 用于加载记录时的 Data（值）容器（复用同一个 DatabaseEntry 实例）。
    private static DatabaseEntry theData = new DatabaseEntry();

    // Encapsulates the environment and databases.
    // 封装了 Berkeley DB 环境与多个数据库句柄的自定义辅助类实例。
    private static MyDbEnv myDbEnv = new MyDbEnv();

    private static void usage() {
        System.out.println("ExampleDatabasePut [-h <env directory>]");
        System.out.println("      [-s <selections file>] [-v <vendors file>]");
        System.exit(-1);
    }

    public static void main(String args[]) {
        ExampleDatabasePut edp = new ExampleDatabasePut();
        try {
            edp.run(args);
        } catch (DatabaseException dbe) {
            System.err.println("ExampleDatabasePut: " + dbe.toString());
            dbe.printStackTrace();
            dbe.printStackTrace();
        } catch (Exception e) {
            System.out.println("Exception: " + e.toString());
            e.printStackTrace();
        } finally {
            myDbEnv.close();
        }
        System.out.println("All done.");
    }

    private void run(String args[])
        throws DatabaseException {
        // Parse the arguments list
        // 解析命令行传入的自定义路径参数
        parseArgs(args);
        // 初始化并打开数据库环境（第二个参数 false 表示环境不是只读的）
        myDbEnv.setup(myDbEnvPath, // path to the environment home
                      false);      // is this environment read-only?

        System.out.println("loading vendors db....");
        loadVendorsDb();

        System.out.println("loading inventory db....");
        loadInventoryDb();
    }
    // 负责从供应商文本文件中读取数据，转换为 Vendor 对象，并通过序列化绑定（SerialBinding）写入供应商数据库。
    private void loadVendorsDb()
            throws DatabaseException {

        // loadFile opens a flat-text file that contains our data
        // and loads it into a list for us to work with. The integer
        // parameter represents the number of fields expected in the
        // file.
        List<String[]> vendors = loadFile(vendorsFile, 8);

        // Now load the data into the database. The vendor's name is the
        // key, and the data is a Vendor class object.

        // Need a serial binding for the data
        EntryBinding dataBinding =
            new SerialBinding(myDbEnv.getClassCatalog(), Vendor.class);

        for (int i = 0; i < vendors.size(); i++) {
            String[] sArray = vendors.get(i);
            Vendor theVendor = new Vendor();
            theVendor.setVendorName(sArray[0]);
            theVendor.setAddress(sArray[1]);
            theVendor.setCity(sArray[2]);
            theVendor.setState(sArray[3]);
            theVendor.setZipcode(sArray[4]);
            theVendor.setBusinessPhoneNumber(sArray[5]);
            theVendor.setRepName(sArray[6]);
            theVendor.setRepPhoneNumber(sArray[7]);

            // The key is the vendor's name.
            // ASSUMES THE VENDOR'S NAME IS UNIQUE!
            String vendorName = theVendor.getVendorName();
            try {
                theKey = new DatabaseEntry(vendorName.getBytes("UTF-8"));
            } catch (IOException willNeverOccur) {}

            // Convert the Vendor object to a DatabaseEntry object
            // using our SerialBinding
            dataBinding.objectToEntry(theVendor, theData);

            // Put it in the database. These puts are transactionally protected
            // (we're using autocommit).
            myDbEnv.getVendorDB().put(null, theKey, theData);
        }
    }

    private void loadInventoryDb()
        throws DatabaseException {

        // loadFile opens a flat-text file that contains our data
        // and loads it into a list for us to work with. The integer
        // parameter represents the number of fields expected in the
        // file.
        List<String[]> inventoryArray = loadFile(inventoryFile, 6);

        // Now load the data into the database. The item's sku is the
        // key, and the data is an Inventory class object.

        // Need a tuple binding for the Inventory class.
        TupleBinding inventoryBinding = new InventoryBinding();

        // Start a transaction. All inventory items get loaded using a
        // single transaction.
        Transaction txn = myDbEnv.getEnv().beginTransaction(null, null);

        for (int i = 0; i < inventoryArray.size(); i++) {
            String[] sArray = inventoryArray.get(i);
            String sku = sArray[1];
            try {
                theKey = new DatabaseEntry(sku.getBytes("UTF-8"));
            } catch (IOException willNeverOccur) {}

            Inventory theInventory = new Inventory();
            theInventory.setItemName(sArray[0]);
            theInventory.setSku(sArray[1]);
            theInventory.setVendorPrice((new Float(sArray[2])).floatValue());
            theInventory.setVendorInventory((new Integer(sArray[3])).intValue());
            theInventory.setCategory(sArray[4]);
            theInventory.setVendor(sArray[5]);

            // Place the Vendor object on the DatabaseEntry object using our
            // the tuple binding we implemented in InventoryBinding.java
            inventoryBinding.objectToEntry(theInventory, theData);

            // Put it in the database. Note that this causes our secondary database
            // to be automatically updated for us.
            try {
                myDbEnv.getInventoryDB().put(txn, theKey, theData);
            } catch (DatabaseException dbe) {
                try {
                System.out.println("Error putting entry " +
                                sku.getBytes("UTF-8"));
                } catch (IOException willNeverOccur) {}
                txn.abort();
                throw dbe;
            }
        }
        // Commit the transaction. The data is now safely written to the
        // inventory database.
        txn.commit();
    }

    private static void parseArgs(String args[]) {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].startsWith("-")) {
                switch(args[i].charAt(1)) {
                  // -h：将紧跟其后的下一个参数赋值给环境路径 myDbEnvPath。
                  case 'h':
                    myDbEnvPath = new File(args[++i]);
                    break;
                  // -i：将紧跟其后的下一个参数赋值给库存文件路径 inventoryFile。
                  case 'i':
                    inventoryFile = new File(args[++i]);
                    break;
                  // -v：将紧跟其后的下一个参数赋值给供应商文件路径 vendorsFile。
                  case 'v':
                    vendorsFile = new File(args[++i]);
                    break;
                  default:
                    usage();
                }
            }
        }
    }

    private List<String[]> loadFile(File theFile, int numFields) {
        List<String[]> records = new ArrayList<String[]>();
        try {
            String theLine = null;
            FileInputStream fis = new FileInputStream(theFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            while((theLine=br.readLine()) != null) {
                String[] theLineArray = theLine.split("#");
                if (theLineArray.length != numFields) {
                    System.out.println("Malformed line found in " + theFile.getPath());
                    System.out.println("Line was: '" + theLine);
                    System.out.println("length found was: " + theLineArray.length);
                    System.exit(-1);
                }
                records.add(theLineArray);
            }
            // Close the input stream handle
            fis.close();
        } catch (FileNotFoundException e) {
            System.err.println(theFile.getPath() + " does not exist.");
            e.printStackTrace();
            usage();
        } catch (IOException e)  {
            System.err.println("IO Exception: " + e.toString());
            e.printStackTrace();
            System.exit(-1);
        }
        return records;
    }

    protected ExampleDatabasePut() {}
}
