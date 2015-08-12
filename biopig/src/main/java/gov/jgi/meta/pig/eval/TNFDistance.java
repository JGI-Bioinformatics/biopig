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

public class TNFDistance extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple input) throws IOException {
		DataBag values = (DataBag) input.get(0);

		if (values.size() == 0)
			return null;

		Tuple t = TupleFactory.getInstance().newTuple(1);
		t.set(0, calculateDistance(values));

		return t;
	}

	private double calculateDistance(DataBag values) throws ExecException {

		double len0 = 0;
		double len1 = 0;
		for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
			Tuple t = it.next();
			Long a = (Long) t.get(0);
			Long b = (Long) t.get(1);
			len0 += a * a;
			len1 += b * b;
		}
		len0 = Math.sqrt(len0);
		len1 = Math.sqrt(len1);
		if (len0 == 0)
			len0 = 1;
		if (len1 == 0)
			len1 = 1;
		double dis = 0;
		for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
			Tuple t = it.next();
			Long a = (Long) t.get(0);
			Long b = (Long) t.get(1);
			double d = a / len0 - b / len1;
			dis += d * d;
		}
		dis = Math.sqrt(dis);
		return dis;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema tokenFs = new Schema.FieldSchema("distance",
					DataType.DOUBLE);
			Schema tupleSchema = new Schema(tokenFs);

			Schema.FieldSchema tupleFs;
			tupleFs = new Schema.FieldSchema("tuple_of_distance", tupleSchema,
					DataType.TUPLE);
			return (new Schema(tupleFs));
		} catch (FrontendException e) {
			// throwing RTE because
			// above schema creation is not expected to throw an exception
			// and also because superclass does not throw exception
			throw new RuntimeException("Unable to compute TOKENIZE schema.");
		}
	}
}
