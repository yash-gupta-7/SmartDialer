from smartdialer.enums import CallStatus, EventClassification
from smartdialer.transitions import classify_call_event

def test_valid_forward_transition_ringing_to_answered():
    result = classify_call_event(CallStatus.RINGING, "ANSWERED", agent_id=5)
    assert result == EventClassification.VALID

def test_duplicate_same_state():
    result = classify_call_event(CallStatus.ANSWERED, "ANSWERED", agent_id=None)
    assert result == EventClassification.DUPLICATE

def test_answered_is_duplicate_once_call_has_progressed_past_it():
    assert classify_call_event(CallStatus.AWAITING_AGENT, "ANSWERED", agent_id=None) == EventClassification.DUPLICATE
    assert classify_call_event(CallStatus.CONNECTED, "ANSWERED", agent_id=5) == EventClassification.DUPLICATE

def test_late_event_after_terminal():
    result = classify_call_event(CallStatus.COMPLETED, "RINGING", agent_id=5)
    assert result == EventClassification.LATE

def test_progression_event_after_connected_is_late_not_impossible():
    # call already moved past raw provider progression (agent-bound, answered and connected);
    # a stray late RINGING must not be treated as a lifecycle-skip anomaly.
    assert classify_call_event(CallStatus.CONNECTED, "RINGING", agent_id=5) == EventClassification.LATE

def test_impossible_transition_skips_lifecycle_before_dialing_started():
    result = classify_call_event(CallStatus.QUEUED, "COMPLETED", agent_id=None)
    assert result == EventClassification.IMPOSSIBLE

def test_pdf_example_sequence_answered_answered_answered_completed():
    # Real sequence: RINGING -> ANSWERED (valid), then two duplicate ANSWERED events,
    # then COMPLETED (valid, terminal reachable once dialing has started).
    state = CallStatus.RINGING
    events = ["ANSWERED", "ANSWERED", "ANSWERED", "COMPLETED"]
    classifications = []
    for ev in events:
        c = classify_call_event(state, ev, agent_id=5)
        classifications.append(c)
        if c == EventClassification.VALID:
            state = CallStatus.ANSWERED if ev == "ANSWERED" else CallStatus(ev)
    assert classifications == [
        EventClassification.VALID,       # RINGING -> ANSWERED
        EventClassification.DUPLICATE,
        EventClassification.DUPLICATE,
        EventClassification.VALID,       # ANSWERED -> COMPLETED
    ]

def test_pdf_example_sequence_completed_answered_ringing():
    state = CallStatus.COMPLETED
    assert classify_call_event(state, "ANSWERED", agent_id=5) == EventClassification.LATE
    assert classify_call_event(state, "RINGING", agent_id=5) == EventClassification.LATE
