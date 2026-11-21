"""Revision-specific limits. Archived experiments retain their original policy."""
def flexible(case):
    return case.get('task_dispatch') and case.get('task_revision') in ('r15', 'r16', 'r17')

def receipt_safe(case):
    """r16 receipt attribution and explicit collaboration closure semantics."""
    return case.get('task_dispatch') and case.get('task_revision') in ('r16', 'r17')

def structured_submission(case):
    return case.get('task_dispatch') and case.get('task_revision') == 'r17'

def window_seconds(case):
    return 1800 if flexible(case) else 3600

def decision_limit(case):
    return None if flexible(case) else 64

def budget_open(case, node):
    import time
    limit = decision_limit(case)
    return ((limit is None or case['decisions'][node] < limit)
            and time.time() < case['created'] + window_seconds(case))
