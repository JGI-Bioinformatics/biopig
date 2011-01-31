//
// Generated stub from file:/Users/dev/dev/git.metagenomics/meta/src/main/groovy/gov/jgi/meta/command/command.groovy
//

package gov.jgi.meta.command;

import java.lang.*;
import java.io.*;
import java.net.*;
import java.util.*;
import groovy.lang.*;
import groovy.util.*;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * simple command interface for all commands supported by the commandline tool
 */
public interface command
{
    /**
     * returns the name of the command (what the user types on commandline)
     */
    java.lang.String name();

    /**
     * returns the set of options that are supported by the command
     *
     * @return a list of flags and a list of parameters eg. [['-flag1'],['-p']]
     */
    List options();

    /**
     * returns the usage of the command
     */
    java.lang.String usage();

    /**
     * executes the command with given arguments and options
     */
    int execute(List args, Map options);
}
