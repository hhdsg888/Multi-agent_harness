"""Revision-specific limits. Archived experiments retain their original policy."""
def flexible(case):
    return case.get('task_dispatch') and case.get('task_revision') == 'r15'

def window_seconds(case):
    return 1800 if flexible(case) else 3600

def decision_limit(case):
    return None if flexible(case) else 64

def budget_open(case, node):
    import time
    limit = decision_limit(case)
    return ((limit is None or case['decisions'][node] < limit)
            and time.time() < case['created'] + window_seconds(case))
