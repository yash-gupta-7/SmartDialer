from smartdialer.enums import AgentStatus, CallStatus, EventClassification, TERMINAL_CALL_STATUSES

VALID_AGENT_TRANSITIONS: dict[AgentStatus, set[AgentStatus]] = {
    AgentStatus.OFFLINE: {AgentStatus.AVAILABLE},
    AgentStatus.AVAILABLE: {AgentStatus.RESERVED, AgentStatus.PAUSED, AgentStatus.OFFLINE},
    AgentStatus.RESERVED: {AgentStatus.DIALING, AgentStatus.AVAILABLE},
    AgentStatus.DIALING: {AgentStatus.CONNECTED, AgentStatus.AVAILABLE, AgentStatus.WRAP_UP},
    AgentStatus.CONNECTED: {AgentStatus.WRAP_UP},
    AgentStatus.WRAP_UP: {AgentStatus.AVAILABLE, AgentStatus.PAUSED, AgentStatus.OFFLINE},
    AgentStatus.PAUSED: {AgentStatus.AVAILABLE, AgentStatus.OFFLINE},
}

# Raw provider-observable progression only — the provider never emits a "CONNECTED" event.
PROGRESSION_ORDER = [CallStatus.QUEUED, CallStatus.RESERVED, CallStatus.INITIATED,
                      CallStatus.RINGING, CallStatus.ANSWERED]
PROGRESSION_EVENT_TARGET = {
    "INITIATED": CallStatus.INITIATED,
    "RINGING": CallStatus.RINGING,
    "ANSWERED": CallStatus.ANSWERED,
}
TERMINAL_EVENT_TARGET = {
    "COMPLETED": CallStatus.COMPLETED,
    "FAILED": CallStatus.FAILED,
    "CANCELLED": CallStatus.CANCELLED,
}
# Exported for events.py: combined event_type -> target-status lookup for VALID events.
EVENT_TARGET_STATUS = {**PROGRESSION_EVENT_TARGET, **TERMINAL_EVENT_TARGET}

# A call in one of these statuses has already moved past raw provider progression events
# (agent-bound calls skip straight to CONNECTED; predictive calls detour through AWAITING_AGENT).
POST_PROGRESSION_STATUSES = {CallStatus.CONNECTED, CallStatus.AWAITING_AGENT}
PRE_DIAL_STATUSES = {CallStatus.QUEUED, CallStatus.RESERVED}

def classify_call_event(current: CallStatus, event_type: str, agent_id: int | None) -> EventClassification:
    # Duplicate: event reasserts the state we are already in or have already passed through.
    if event_type == current.value:
        return EventClassification.DUPLICATE
    if event_type == "ANSWERED" and current in (CallStatus.ANSWERED, CallStatus.AWAITING_AGENT, CallStatus.CONNECTED):
        return EventClassification.DUPLICATE

    # Terminal states are sticky: anything arriving after a terminal state is late, never applied.
    if current in TERMINAL_CALL_STATUSES:
        return EventClassification.LATE

    if event_type in TERMINAL_EVENT_TARGET:
        # COMPLETED/FAILED/CANCELLED are reachable from any state once dialing actually
        # started; reaching them from QUEUED/RESERVED means the call was never dialed.
        if current in PRE_DIAL_STATUSES:
            return EventClassification.IMPOSSIBLE
        return EventClassification.VALID

    if event_type not in PROGRESSION_EVENT_TARGET:
        return EventClassification.IMPOSSIBLE

    if current in POST_PROGRESSION_STATUSES:
        # call already progressed past raw provider events (e.g. CONNECTED); a stray
        # RINGING/ANSWERED here is out-of-order, not a lifecycle-skip anomaly.
        return EventClassification.LATE

    target = PROGRESSION_EVENT_TARGET[event_type]
    current_idx = PROGRESSION_ORDER.index(current)
    target_idx = PROGRESSION_ORDER.index(target)
    if target_idx == current_idx + 1:
        return EventClassification.VALID
    if target_idx <= current_idx:
        return EventClassification.LATE
    # skips one or more lifecycle steps forward
    return EventClassification.IMPOSSIBLE
