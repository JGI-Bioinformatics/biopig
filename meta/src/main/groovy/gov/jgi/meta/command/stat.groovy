/*
 * Copyright (c) 2010, Joint Genome Institute (JGI) United States Department of Energy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by the JGI.
 * 4. Neither the name of the JGI nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
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

import gov.jgi.meta.MetaUtils


class statCommand implements command {

    List flags = [

    ]

    List params = [
            '--filter'   // number of reads
    ]


    String name() {
        return "stat"
    }

    List options() {

      /* return list of flags (existential) and parameters  */
      return [
              flags, params
      ];

    }

    String usage() {
        return "stat <input file/dir> [--filter <n>] ";
    }

    int execute(List args, Map options) {

      int numBases;
      int numSequences;

      Map<String, String> contigs1 = MetaUtils.readSequences(args[1]);
      if (options['-d']) {
        println("read " + args[1] + ": " + contigs1.size() + " sequences")
      }

      int maxCount = (options['--filter'] ? options['--filter'] : contigs1.size());

      int count = 0;
      for (String k : contigs1.keySet()) {
        count++;

        String s1 = contigs1.get(k);

        println(">" + k + "\n" + s1);

        if (count >= maxCount) break;
      }

        return 1;
    }

}