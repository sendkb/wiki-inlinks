package com.a9.cs.pig.fblink;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;


public class JsonArrayTOBag extends EvalFunc<DataBag> {
	BagFactory mBagFactory = BagFactory.getInstance();
	TupleFactory mTupleFactory = TupleFactory.getInstance();


	@Override
	public DataBag exec(Tuple input) throws IOException {

		DataBag output = mBagFactory.newDefaultBag();
		if (input.size() > 0 && input.get(0) != null) {
			String jsonArray = (String) input.get(0);
			Object obj = JSONValue.parse(jsonArray);
			JSONArray array=(JSONArray)obj;

			Iterator<?> iter = array.iterator();

			while (iter.hasNext()) {
				output.add(mTupleFactory.newTuple(NormalizeString.normalizeToUUID((String) iter.next())));
			}
		}

		return output;
	}
}
