"""Pure resource arithmetic on explicitly observed/published node facts.

Lists alternatives without choosing one, diagnosing a fault, or changing state.
These formulas implement this two-end simulator contract, not a radio model.
"""
def link_options(observations,goal):
    minimum=goal['video_min_mbps'];desired=goal['video_desired_mbps']
    voice=max(o['measurements']['voice_demand_mbps'] for o in observations.values())
    paths=set.intersection(*(set(o['ports']) for o in observations.values()))
    rows=[]
    for path in sorted(paths):
        up=all(o['ports'][path]['physical_up'] for o in observations.values())
        limits={n:o['ports'][path]['capacity_mbps']-o['background_mbps']-voice for n,o in observations.items()}
        maximum=min(desired,*limits.values())
        if set(observations)=={'A','B'}:
            # Plant.measure subtracts B's background from the end-to-end bottleneck.
            capacity=min(o['ports'][path]['capacity_mbps'] for o in observations.values())
            maximum=min(maximum,capacity-observations['B']['background_mbps']-voice)
        maximum=max(0.,maximum);feasible=up and maximum>=minimum
        rows.append({'path':path,'physical_up_at_observation':up,'video_rate_range_mbps':[minimum,maximum] if feasible else None,
                     'max_video_mbps':maximum if up else 0.,'minimum_resource_requirement_met':feasible,
                     'configurable_common_channels':['c1','c2'],'management_enabled_at_observation':{n:o['ports'][path]['enabled'] for n,o in observations.items()}})
    return {'scope':'group' if len(observations)==2 else 'local','observation_sources':{n:{'observation_id':o['observation_id'],'sampled_at':o['sampled_at'],'config_version':o['config_version'],'environment_revision':o['environment_revision']} for n,o in observations.items()},
            'voice_reserved_mbps':voice,'options':rows,'chosen_option':None,'configuration_applied':False,
            'limitations':'Observed resource arithmetic; local-only options do not prove peer feasibility. State can change; prepare/apply still validate versions.'}
