"""Validate public argument contracts without filling or correcting values."""
def validate(value,schema,path='arguments'):
    kind=schema.get('type')
    valid={'object':isinstance(value,dict),'string':isinstance(value,str),
           'number':type(value) in (int,float),'integer':type(value) is int,
           'boolean':type(value) is bool,'array':isinstance(value,list)}
    if kind and not valid.get(kind,False):return path+' must be '+kind
    if 'enum' in schema and value not in schema['enum']:return path+' outside allowed enum'
    if kind=='object':
        props=schema.get('properties',{})
        for key in schema.get('required',[]):
            if key not in value:return path+'.'+key+' is required'
        for key,item in value.items():
            if key not in props:
                if schema.get('additionalProperties') is False:return path+'.'+key+' is not allowed'
                continue
            error=validate(item,props[key],path+'.'+key)
            if error:return error
    return None

def model_history(messages,limit=32):
    # Keep native call/result pairs together at the bounded history boundary.
    start=max(2,len(messages)-limit)
    while start>2 and messages[start].get('role')=='tool':start-=1
    return messages[:2]+messages[start:]
