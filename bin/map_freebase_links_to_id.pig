%default base_path  's3://..'  -- specify the s3 bucket
%default input_path  '$base_path/input-files/test-input.txt'
%default output_path '$base_path/freebase_processed_output/'

register '$base_path/pig-jars/slf4j-log4j12-1.5.0.jar'                                       
register '$base_path/pig-jars/slf4j-api-1.5.10.jar'                                          
register '$base_path/pig-jars/google-collect-1.0.jar'                                        
register '$base_path/pig-jars/json-simple-1.1.jar'                                           
register '$base_path/pig-jars/elephant-bird-pig-3.0.2.jar'  
register '$base_path/pig-jars/fblink-0.0.1-SNAPSHOT.jar'

SET default_parallel 3 --number of reducers

-- Load input data using twitter's json loader
inp = LOAD '$input_path' USING com.twitter.elephantbird.pig.load.JsonLoader();

-- Convert the jsonString to pig map for us to access data in pig land
inp_with_fields = FOREACH inp GENERATE (CHARARRAY)$0#'id' AS id, com.twitter.elephantbird.pig.piggybank.JsonStringToMap((CHARARRAY)$0#'fields'), $0 as data;

--project only necessary fields
id_data_table = FOREACH inp_with_fields GENERATE id, data;
id_field_table = FOREACH inp_with_fields GENERATE id, $1 AS field;

--Convert json array to bag for flattening it out
id_outlink_table_verbose = FOREACH id_field_table GENERATE id, FLATTEN(com.a9.cs.pig.fblink.JsonArrayTOBag(field#'outlinks')) AS outlink;
id_outlink_table = FILTER id_outlink_table_verbose BY outlink IS NOT NULL;
id_name_table    = FOREACH inp_with_fields GENERATE id, $1#'name' AS name;

--id_name_link_joined: {id_name_table::id: chararray,id_name_table::name: chararray,id_outlink_table::id: chararray,id_outlink_table::outlink: bytearray}
id_name_link_joined_verbose = JOIN id_name_table BY name, id_outlink_table BY outlink;

id_name_link_joined = FOREACH id_name_link_joined_verbose GENERATE id_name_table::id AS doc_id, id_outlink_table::id AS outlink_id;
id_link_group = GROUP id_name_link_joined BY doc_id;

--id_data_joined: {id_data_table::id: chararray,id_data_table::data: bytearray,id_link_group::group: chararray,id_link_group::id_name_link_joined: {(doc_id: chararray,outlink_id: chararray)}}
id_data_joined_verbose = JOIN id_data_table BY id LEFT, id_link_group BY group;
id_data_joined = FOREACH id_data_joined_verbose GENERATE id_data_table::data AS data, id_link_group::id_name_link_joined.outlink_id AS outlink_ids;

all_out = FOREACH id_data_joined GENERATE com.a9.cs.pig.fblink.AppendOutlinksToSDF(data, outlink_ids);

rmf $output_path;
STORE all_out INTO '$output_path' USING PigStorage(); 
