"""Flat native action/arguments syntax; per-tool semantics are checked on execution.

Original transport and vendor bytes stay unchanged. Public catalog keeps each
tool's complete schema. This avoids depending on conditional native grammar
support; it does not repair outputs, suppress model EOS or select business actions.
"""
from collab_lab import providers as transport

def complete_decision_schema(actions,schemas=None):
    # Flat root contract is supported by native JSON grammar. Per-tool required
    # business arguments remain in the public catalog and execution validation.
    properties={}
    for schema in (schemas or {}).values():properties.update(schema.get('properties',{}))
    return {'type':'object','properties':{'action':{'type':'string','enum':list(actions)},'arguments':{'type':'object','properties':properties,'additionalProperties':False},'reason':{'type':'string'}},'required':['action','arguments'],'additionalProperties':False}

# All users of this module are isolated node processes; original vendor bytes stay immutable.
transport.decision_schema=complete_decision_schema
NodeProviders=transport.Providers
