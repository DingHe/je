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

package je.rep.quote;

import java.util.StringTokenizer;

/**
 * An enumeration of the commands used by the stock quotes example.
 */
// Command 是一个在 Berkeley DB JE 高可用（HA）股票报价示例中使用的控制台命令解析与分发枚举类。
// 定义支持的交互命令：规范了股票报价应用中允许用户输入的各种控制台指令（如打印所有库存、退出应用、以及更新股票价格）。
// 输入词法校验与解析：提供静态解析方法 getCommand(String line)，负责将用户在控制台输入的原始字符串按空格进行分词，识别出对应的命令类型，并校验参数格式（例如确保更新股票时价格必须是合法的数字，且不允许有多余的未知参数），若格式错误则抛出规范的异常。
//
enum Command {
    // 对应控制台输入的 print 指令。指示系统打印当前数据库中的所有股票信息（传入构造函数的 true 表示它是显式命令/Manifest 命令，不需要额外参数）。
    PRINT(true), /* Prints all the stocks currently in the database */
    // 对应股票更新指令（例如输入 AAPL 150.5）。属于非显式命令（默认传入 false），它需要紧跟股票代码和对应的数值价格。
    UPDATE,      /* Update the info associated with the stock */
    // 对应控制台输入的 quit 指令，用于安全退出当前节点应用程序（显式命令，无需额外参数）。
    QUIT(true),  /* Quit the application */
    // 当用户在控制台直接敲回车输入空行时返回该常量，表示无任何有效命令。
    NONE;        /* An internal pseudo command indicating no command */

    /* Indicates whether the command is manifest, that is its the enum name
       itself.  */
    // 用于区分该命令是否为“显式命令（Manifest）”。
    final private boolean manifest;

    Command(boolean manifest) {
        this.manifest = manifest;
    }

    /**
     * A non-manifest command
     */
    Command() {
        this(false);
    }

    /**
     * Determines the command denoted by the line.
     *
     * @param line the text as typed in at the console.
     *
     * @return the command represented by the line, or NONE if the line is
     * empty.
     *
     * @throws InvalidCommandException if no recognizable command was found on
     * a non-empty line.
     */
    // 负责将控制台输入的原始文本行（line）解析为对应的 Command 枚举，并对输入格式进行严格校验。
    static Command getCommand(String line) throws InvalidCommandException {
        // 对输入的字符串按空白字符进行切分。
        StringTokenizer tokenizer = new StringTokenizer(line);
        // 如果没有解析到任何 Token（即用户直接敲了回车），直接返回 NONE。
        if (!tokenizer.hasMoreTokens()) {
            return NONE;
        }
        // 获取首个 Token：将第一个 Token 作为命令名称（command）。
        String command = tokenizer.nextToken();

        /* Check for a manifest command */
        // 匹配显式命令（Manifest Command）：
        for (Command c : Command.values()) {
            // 如果某个枚举是 manifest 属性为 true 且其名字与输入匹配（不区分大小写 equalsIgnoreCase）：
            if (c.manifest && c.name().equalsIgnoreCase(command)) {
                // 如果没有，说明格式正确，返回该命令（如 PRINT 或 QUIT）。
                if (!tokenizer.hasMoreTokens()) {
                    return c;
                }
                /* Extra token. */
                throw new InvalidCommandException(
                        "Unexpected argument: "  + tokenizer.nextToken() +
                        " for command: " + command);
            }
        }
        // 匹配更新命令（UPDATE）与参数校验：
        /* A stock update command, token following arg must be a price*/
        if (!tokenizer.hasMoreTokens()) {
            throw new InvalidCommandException("Unknown command: " + command +
                                              "\n" + StockQuotes.usage());
        }
        String price = tokenizer.nextToken();

        try {
            Float.parseFloat(price);
            if (tokenizer.hasMoreTokens()) {
                throw new InvalidCommandException
                    ("Extraneous argument: " + tokenizer.nextToken());
            }
        } catch (NumberFormatException e) {
            throw new InvalidCommandException
                ("Stock price must be a numeric value, not: " + price);
        }

        return UPDATE;
    }

    @SuppressWarnings("serial")
    static class InvalidCommandException extends Exception {
        InvalidCommandException(String error) {
            super(error);
        }
    }
}
