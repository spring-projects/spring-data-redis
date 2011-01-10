/*
 * Copyright (c) 2011 by J. Brisbin <jon@jbrisbin.com>
 * Portions (c) 2011 by NPC International, Inc. or the
 * original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.keyvalue.riak.util;

import org.apache.commons.cli.*;
import org.springframework.data.keyvalue.riak.core.RiakTemplate;

import java.io.*;
import java.net.URLEncoder;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakClassFileLoader {

  static Options opts = new Options();

  static {
    opts.addOption("v", false, "Verbose output");
    opts.addOption("u",
        true,
        "URL to Riak (defaults to: 'http://localhost:8098/riak/{bucket}/{key}')");
    opts.addOption("b", true, "Bucket to load class files into");
    opts.addOption("k", true, "Key under which to store an individual class file");
    opts.addOption("j", true, "JAR file to load into Riak");
    opts.addOption("c", true, "Class file to load into Riak");
    opts.addOption("d", true, "Directory from which to load all JAR files into Riak");
  }

  public static void main(String[] args) {
    Parser p = new BasicParser();
    CommandLine cl = null;
    try {
      cl = p.parse(opts, args);
    } catch (ParseException e) {
      System.err.println("Error parsing command line: " + e.getMessage());
    }

    if (null != cl) {
      boolean verbose = cl.hasOption('v');
      RiakTemplate riak = new RiakTemplate();
      riak.getRestTemplate().setErrorHandler(new Ignore404sErrorHandler());
      if (cl.hasOption('u')) {
        riak.setDefaultUri(cl.getOptionValue('u'));
      }
      try {
        riak.afterPropertiesSet();
      } catch (Exception e) {
        System.err.println("Error creating RiakTemplate: " + e.getMessage());
      }
      String[] files = cl.getOptionValues('j');
      if (null != files) {
        for (String file : files) {
          if (verbose) {
            System.out.println(String.format("Loading JAR file %s into Riak...", file));
          }
          try {
            File zfile = new File(file);
            ZipInputStream zin = new ZipInputStream(new FileInputStream(zfile));
            ZipEntry entry;
            while (null != (entry = zin.getNextEntry())) {
              ByteArrayOutputStream bout = new ByteArrayOutputStream();
              byte[] buff = new byte[16384];
              for (int bytesRead = zin.read(buff); bytesRead > 0; bytesRead = zin.read(buff)) {
                bout.write(buff, 0, bytesRead);
              }

              if (entry.getName().endsWith(".class")) {
                String name = entry.getName().replaceAll("/", ".");
                name = URLEncoder.encode(name.substring(0, name.length() - 6), "UTF-8");
                String bucket;
                if (cl.hasOption('b')) {
                  bucket = cl.getOptionValue('b');
                } else {
                  bucket = URLEncoder.encode(zfile.getCanonicalFile().getName(), "UTF-8");
                }
                if (verbose) {
                  System.out.println(String.format("Uploading to %s/%s", bucket, name));
                }

                // Load these bytes into Riak
                riak.setAsBytes(bucket, name, bout.toByteArray());
              }
            }
          } catch (FileNotFoundException e) {
            System.err.println("Error reading JAR file: " + e.getMessage());
          } catch (IOException e) {
            System.err.println("Error reading JAR file: " + e.getMessage());
          }
        }
      }

      String[] classFiles = cl.getOptionValues('c');
      if (null != classFiles) {
        for (String classFile : classFiles) {
          try {
            FileInputStream fin = new FileInputStream(classFile);
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            byte[] buff = new byte[16384];
            for (int bytesRead = fin.read(buff); bytesRead > 0; bytesRead = fin.read(buff)) {
              bout.write(buff, 0, bytesRead);
            }

            String name;
            if (cl.hasOption('k')) {
              name = cl.getOptionValue('k');
            } else {
              throw new IllegalStateException(
                  "Must specify a Riak key in which to store the data if loading individual class files.");
            }
            String bucket;
            if (cl.hasOption('b')) {
              bucket = cl.getOptionValue('b');
            } else {
              throw new IllegalStateException(
                  "Must specify a Riak bucket in which to store the data if loading individual class files.");
            }
            if (verbose) {
              System.out.println(String.format("Uploading to %s/%s", bucket, name));
            }

            // Load these bytes into Riak
            riak.setAsBytes(bucket, name, bout.toByteArray());

          } catch (FileNotFoundException e) {
            System.err.println("Error reading class file: " + e.getMessage());
          } catch (IOException e) {
            System.err.println("Error reading class file: " + e.getMessage());
          }
        }
      }
    }
  }

}
