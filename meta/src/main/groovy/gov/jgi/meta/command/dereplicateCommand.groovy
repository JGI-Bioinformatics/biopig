/*
 * Copyright (c) 2010, Joint Genome Institute (JGI) United States Department of Energy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *     must display the following acknowledgement:
 *     This product includes software developed by the JGI.
 * 4. Neither the name of the JGI nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY JGI ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL JGI BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */





package gov.jgi.meta.command

/**
  * wrapper for the hadoop assembler application
  */
class dereplicateCommand implements command {

    List flags = [

    ]

    List params = [

    ]


    String name() {
        return "dereplicate"
    }

    List options() {

      /* return list of flags (existential) and parameters  */
      return [
              flags, params
      ];

    }

    String usage() {
        return "dereplicate <readfastafile> <outputdir>";
    }

    int execute(List args, Map options) {

      String metaHome = System.getProperty("meta.home").replaceFirst("~", System.getenv("HOME"));
      String pbsJobId = System.getenv("PBS_JOBID");

      StringBuilder command = new StringBuilder("hadoop ");

      /*
      add special config directory when used at nersc through PBS
       */
      if (pbsJobId != null) {
         println("using hadoop config dir: " + pbsJobId);
         command.append("--config " + System.getenv("HOME") + "/.hadoop-"+pbsJobId + " jar");
       } else {
         command.append(" jar");
       }

      /*
      find the jar file to execute
       */
      String jar = new File(metaHome + "/hadoop").listFiles(
              {dir, file-> file ==~ /dereplicateHadoopApp-.*-job.jar/ } as FilenameFilter
      ).sort {it.lastModified() }.reverse()[0];

      command.append(" " + jar + " gov.jgi.meta.Dereplicate")
      command.append(" " + args[1] + " " + args[2])

      /*
      execute command and pipe stdout/stderr to local stdout/stderr
       */
      String commandStr = command.toString();
      if (options['-d']) {
         println("command: " + commandStr);
      }
      Process proc = commandStr.execute()
      proc.consumeProcessErrorStream(System.err);
      proc.consumeProcessOutputStream(System.out);
      proc.waitFor()

      return proc.exitValue();
    }

}