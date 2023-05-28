/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallerContext;

/**
 * A utility to help run {@link Tool}s.
 *
 * <p><code>ToolRunner</code> can be used to run classes implementing 
 * <code>Tool</code> interface. It works in conjunction with 
 * {@link GenericOptionsParser} to parse the 
 * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options">
 * generic hadoop command line arguments</a> and modifies the 
 * <code>Configuration</code> of the <code>Tool</code>. The 
 * application-specific options are passed along without being modified.
 * </p>
 *
 * @see Tool
 * @see GenericOptionsParser
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ToolRunner {

    /**
     * Runs the given <code>Tool</code> by {@link Tool#run(String[])}, after
     * parsing with the given generic arguments. Uses the given
     * <code>Configuration</code>, or builds one if null.
     *
     * Sets the <code>Tool</code>'s configuration with the possibly modified
     * version of the <code>conf</code>.
     *
     * @param conf <code>Configuration</code> for the <code>Tool</code>.
     * @param tool <code>Tool</code> to run.
     * @param args command-line arguments to the tool.
     * @return exit code of the {@link Tool#run(String[])} method.
     */
    public static int run(Configuration conf, Tool tool, String[] args)
            throws Exception {
        if (CallerContext.getCurrent() == null) {
            CallerContext ctx = new CallerContext.Builder("CLI").build();
            CallerContext.setCurrent(ctx);
        }

        // 对于 JournalNode 启动 conf 默认值 null
        if (conf == null) {
            // 创建 Configuration 对象 并将 core-default.xml core-site.xml 文件名添加到 defaultResources 容器中
            conf = new Configuration();
        }
        // 解析入参 args
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        // set the configuration back, so that Tool can configure itself
        // 对于 JournalNode 回设 Configuration 对象 底层加载四大文件 (hdfs-default.xml hdfs-site.xml core-default.xml core-site.xml)
        tool.setConf(conf);

        //get the args w/o generic hadoop args
        // 解析 main() 入参
        String[] toolArgs = parser.getRemainingArgs();
        // 对于 JournalNode 调用其 run()
        return tool.run(toolArgs);
    }

    /**
     * Runs the <code>Tool</code> with its <code>Configuration</code>.
     *
     * Equivalent to <code>run(tool.getConf(), tool, args)</code>.
     *
     * @param tool <code>Tool</code> to run.
     * @param args command-line arguments to the tool.
     * @return exit code of the {@link Tool#run(String[])} method.
     */
    public static int run(Tool tool, String[] args)
            throws Exception {
        // 往下追
        return run(
                // 对于 JournalNode 默认值 null
                tool.getConf(),
                tool, args);
    }

    /**
     * Prints generic command-line argurments and usage information.
     *
     *  @param out stream to write usage information to.
     */
    public static void printGenericCommandUsage(PrintStream out) {
        GenericOptionsParser.printGenericCommandUsage(out);
    }


    /**
     * Print out a prompt to the user, and return true if the user
     * responds with "y" or "yes". (case insensitive)
     */
    public static boolean confirmPrompt(String prompt) throws IOException {
        while (true) {
            System.err.print(prompt + " (Y or N) ");
            StringBuilder responseBuilder = new StringBuilder();
            while (true) {
                int c = System.in.read();
                if (c == -1 || c == '\r' || c == '\n') {
                    break;
                }
                responseBuilder.append((char) c);
            }

            String response = responseBuilder.toString();
            if (response.equalsIgnoreCase("y") ||
                    response.equalsIgnoreCase("yes")) {
                return true;
            } else if (response.equalsIgnoreCase("n") ||
                    response.equalsIgnoreCase("no")) {
                return false;
            }
            System.err.println("Invalid input: " + response);
            // else ask them again
        }
    }
}
