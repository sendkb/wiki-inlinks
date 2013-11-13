package com.a9.cs.pig.fblink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class AppendOutlinksToSDF extends EvalFunc<Tuple> {
	BagFactory mBagFactory = BagFactory.getInstance();
	TupleFactory mTupleFactory = TupleFactory.getInstance();

	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple output = mTupleFactory.newTuple();
		if (input.size() == 2) {
			Map<String,Object> jsonMap = (Map<String, Object>) input.get(0);
			DataBag outlinkIds = (DataBag) input.get(1);
			Set<String> uniqueIds = new HashSet<String>();
			List<Object> list = new ArrayList<Object>();
			int cnt = 0;
			if (outlinkIds != null) {
				for (Tuple t : outlinkIds) {
					if (cnt > 100) {
						break;
					}
					cnt++;
					uniqueIds.add(t.get(0).toString());
				}
				list.addAll(uniqueIds);
				Map<String,Object> fieldMap = (Map<String, Object>) jsonMap.get("fields");				
				fieldMap.put("outlink_ids", list);
				jsonMap.put("fields", fieldMap);
			}
			Map<String, Object> outputMap = new HashMap<String, Object>();
			for (Entry<String, Object> item : jsonMap.entrySet()) {
				if (item.getKey().equals("fields")) {
					try {
						JSONObject json = (JSONObject)new JSONParser().parse(item.getValue().toString());
						Map<String, Object> innerMap = new HashMap<String, Object>();
						for (Object jsonKeys : json.keySet()) {
							if (String.class.isInstance(jsonKeys) && 
									!Map.class.isInstance(json.get(jsonKeys).getClass())) {
								//We only handle string key and non-map values. 
								//i.e. all values inside field are only one level deep
								//Check with pnag@
								innerMap.put(jsonKeys.toString(), json.get(jsonKeys));
							} else {
								//Unknown key type
							}
						}
						outputMap.put("fields", innerMap);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else {
					outputMap.put(item.getKey(), item.getValue());
				}
			}

			JSONObject outJsonObj = new JSONObject();
			outJsonObj.putAll(outputMap);
			output.append(outJsonObj.toJSONString());
		}
		return output;
	}
}

