from another import getting_value_schema,getting_key_schema
var_server_name = 'ashaplq00003'
var_topic_name = 'NBC_APPS.TBL_MS_ADVERTISER'
var_port_no = '8081'

var_result =getting_value_schema(var_server_name, var_topic_name,var_port_no)
print(var_result)
var_resulta =getting_key_schema(var_server_name, var_topic_name,var_port_no)
print(var_resulta)
