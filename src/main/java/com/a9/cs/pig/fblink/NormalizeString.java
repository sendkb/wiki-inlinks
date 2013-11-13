package com.a9.cs.pig.fblink;

import java.io.IOException;
import java.util.UUID;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class NormalizeString extends EvalFunc<Tuple>
{
	TupleFactory mTupleFactory = TupleFactory.getInstance();

	public static String normalizeToUUID(String input) {
		String output = null;
		if (input != null) {
			output = input.toLowerCase().trim();
			UUID u1 = UUID.nameUUIDFromBytes(output.getBytes());
			return u1.toString();
		}
		return null;

	}
	
	@Override
	public Tuple exec(Tuple input) throws IOException {

		Tuple output = mTupleFactory.newTuple();
		if (input.size() > 0 && input.get(0) != null) {
			output.append(NormalizeString.normalizeToUUID((String) input.get(0)));
		}
		return output;
		
	}
}
