mylist = [{'name': 'table', 'type': ['null', 'string']}, {'name': 'op_type', 'type': ['null', 'string']}, {'name': 'op_ts', 'type': ['null', 'string']}, {'name': 'current_ts', 'type': ['null', 'string']}, {'name': 'pos', 'type': ['null', 'string']}, {'name': 'ADVERTISER_ID', 'type': ['null', 'long']}, {'name': 'ADVERTISER_NAME', 'type': ['null', 'string']}, {'name': 'ADVERTISER_CREATION_DT', 'type': ['null', 'string']}, {'name': 'ADVERTISER_MODIFICATION_DT', 'type': ['null', 'string']}, {'name': 'CREATE_DATE', 'type': ['null', 'string']}, {'name': 'ACCOUNTING_IDENT', 'type': ['null', 'string']}]

import json 

results = []

current_dict = {'name': 'op_type', 'type': ['null', 'string']}

def replace_value_with_definition(key_to_find, definition):
    for key in current_dict.values():
        if key == key_to_find:
           current_dict[key] = definition

replace_value_with_definition('type', 'StringType')


for x in mylist:
    var_dict = x
    r = json.dumps(var_dict)
    loaded_r = json.loads(r)
    print(loaded_r)
    print(type(loaded_r))
    type1 = loaded_r['type']
    var_data_raw = type1[1]
    name = loaded_r['name']
    var_nullable = type1[0]
    var_data_type = type1[1]
    var_Str_field = 'StructField('
    var_name_field = '"{}"'.format(name)
    var_null_bolen  = ',nullable='
    var_schema_val = var_Str_field+var_name_field+','+var_data_type+var_null_bolen+var_nullable+')'
    results.append(var_schema_val)

print(results)


