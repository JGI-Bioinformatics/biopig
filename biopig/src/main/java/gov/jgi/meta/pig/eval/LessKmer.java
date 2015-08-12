package gov.jgi.meta.pig.eval;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class LessKmer extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		String value = (String) input.get(0);
		StringBuffer a = new StringBuffer();
		char[] chars = value.toCharArray();
		for (int i = 0; i < chars.length; i++) {
			char x;
			switch (chars[i]) {
			case 'a':
				x = 't';
				break;
			case 'c':
				x = 'g';
				break;
			case 'g':
				x = 'c';
				break;
			case 't':
				x = 'a';
				break;
			default:
				throw new ExecException("don't know " + chars[i]);
			}

			a.append(x);
		}
		
		String complmentkmer = new StringBuilder(a).reverse().toString();
		if (value.compareTo(complmentkmer)>0){
			return complmentkmer;
		} else{
			return value;
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		  return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
