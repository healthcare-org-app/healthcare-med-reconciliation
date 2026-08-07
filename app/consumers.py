"""Kafka consumers for med-reconciliation-service.

One handler per subscribed topic. Handlers are best-effort logging plus
audit — services override this file to implement real cross-domain behavior.
"""
from __future__ import annotations

import logging

from healthcare_common.audit import emit_audit

log = logging.getLogger("med-reconciliation-service.consumers")


def register(svc) -> None:
    bus = svc.bus

    @bus.on("encounter.started")
    def _on_encounter_started(envelope: dict) -> None:
        log.info("med-reconciliation-service: received encounter.started id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.encounter.started", actor="system:med-reconciliation-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.issued")
    def _on_prescription_issued(envelope: dict) -> None:
        log.info("med-reconciliation-service: received prescription.issued id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.prescription.issued", actor="system:med-reconciliation-service",
                   target=None, details={"envelope_id": envelope.get("id")})

