package gov.jgi.meta.pig.eval;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import java.util.Iterator;
import org.apache.pig.data.*;

public class GenReadPair_Long extends EvalFunc<DataBag> {
    //Is this for the log ? I copyed this from KmerGenerator.java.
    private static final Log LOG = LogFactory.getLog(GenReadPair_Long.class);

    public DataBag exec(Tuple input) throws IOException{
	//Put all the output values into a DataBag, like in KmerGenerator.java.
	DataBag output = DefaultBagFactory.getInstance().newDefaultBag();

	if ((input == null) || (input.size() == 0)){
	    return(null);
	}

	try{
	    int i=0;//loop counter
	    DataBag bagInput = (DataBag)input.get(0);//get the input databag
	    //bagInput: {Tuple(chararray)}
	    int inputSize = (int)bagInput.size();

	    //Keep the input data into a Tuple, so it can be indexed.
	    Tuple tupleInput = DefaultTupleFactory.getInstance().newTuple(inputSize);
	    Iterator bagIt = bagInput.iterator();
	    Tuple t;//for iteration
	    while (bagIt.hasNext()){
		t = (Tuple)bagIt.next();
		if(t != null && t.size() > 0 && t.get(0) != null){
		    tupleInput.set(i, (long)t.get(0));
		    i++;
		    if(i >= inputSize)//there may need some warning
			break;
		}
	    }

	    //Walk through bagInput, gen read pairs and append them to output one by one
	    int j;//another loop counter
	    //String read0, read1;
	    for (i=0; i<inputSize; i++){
		for (j=i+1; j<inputSize; j++){
		    long read0, read1;
		    read0 = (long)tupleInput.get(i);
		    read1 = (long)tupleInput.get(j);
		    if ( read0 == read1 ) continue;
		    
		    Tuple readPair = DefaultTupleFactory.getInstance().newTuple(2);
		    if(read0 < read1){
			readPair.set(0, read0);
			readPair.set(1, read1);
		    }else{
			readPair.set(0, read1);
			readPair.set(1, read0);
		    }
		    output.add(readPair);
		}
	    }
	    
	} catch (Exception e) {
	    System.err.println("GenReadPair: failed to process input; error - "+ e.getMessage());
	    return(null);
	}
	return(output);
    }
}
