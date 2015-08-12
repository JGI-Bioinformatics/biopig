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

public class TNFLength extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple input) throws IOException {
		DataBag values = (DataBag) input.get(0);

		if (values.size() == 0)
			return null;

		Tuple t = TupleFactory.getInstance().newTuple(1);
		t.set(0, calculateLength(values));

		return t;
	}

	private double calculateLength(DataBag values) throws ExecException {

		double len0 = 0;
		for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
			Tuple t = it.next();
			Long a = (Long) t.get(1);
			len0 += a * a;
		}
		len0 = Math.sqrt(len0);
		return len0;
	}

	@Override
	public Schema outputSchema(Schema input) {
		  return new Schema(new Schema.FieldSchema("length", DataType.DOUBLE));
	}
}
