"""Give each native grammar alternative its complete public object contract.

The original 1.1.2 transport is retained unchanged. Its original schema placed
required/type at the root but only properties in oneOf alternatives. Some native
grammar implementations select an alternative before applying root constraints.
This experimental adapter repeats the full requirements inside every alternative.
It does not repair responses or select actions/arguments.
"""
from collab_lab import providers as transport

def complete_decision_schema(actions,schemas=None):
    branches=[]
    for name in actions:
        branches.append({'type':'object','properties':{'action':{'type':'string','enum':[name]},'arguments':(schemas or {})[name],'reason':{'type':'string'}},'required':['action','arguments'],'additionalProperties':False})
    return {'oneOf':branches}

# All users of this module are isolated node processes; original vendor bytes stay immutable.
transport.decision_schema=complete_decision_schema
NodeProviders=transport.Providers
