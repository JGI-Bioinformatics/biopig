/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.jgi.meta.pig.eval;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

/**
 * Return a long hash from a String  --lanhin
 */
public class HashLong extends EvalFunc<Long> {
    private static final Log LOG = LogFactory.getLog(HashLong.class );

    public Long exec(Tuple input) throws IOException
    {
	long h = 1125899906842597L; // prime

	String in;
	if ((input == null) || (input.size() == 0))
	    {
		return 0L;
	    }

	try{
	    Object values = input.get(0);

	    if (values instanceof DataByteArray) {
		byte[] in_bytes = ((DataByteArray)values).get();
		in = new String(in_bytes, "ISO-8859-1");
	    } else {
		in = (String)values;
	    }

	    int len = in.length();
	    
	    for (int i = 0; i < len; i++) {
		// Should ((h<<5) - h) instead of 31*h be faster?  --lanhin
		//h = 31*h + in.charAt(i);
		h = ((h<<5) - h) + in.charAt(i);
	    }
	}catch (Exception e) {
	    System.err.println("HashLong: failed to process input; error - " + e.getMessage());
	    return 0L;
	}
	return h;
    }

}
