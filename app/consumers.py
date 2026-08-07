"""Kafka consumers for med-reconciliation-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("med-reconciliation-service.consumers")

TABLE = "med_reconciliation"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("encounter.started")
    def _on_encounter_started(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"encounter_id": data.get("id"),
                                      "patient_id":   data.get("patient_id"),
                                      "state": "reconciliation_pending"}),))
        except Exception as e:
            log.exception("med-reconciliation-service/encounter.started handler failed: %s", e)
        emit_audit(bus, action="consume.encounter.started", actor="system:med-reconciliation-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.issued")
    def _on_prescription_issued(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"kind": "new_med",
                                      "prescription_id": data.get("id"),
                                      "drug": data.get("drug")}),))
        except Exception as e:
            log.exception("med-reconciliation-service/prescription.issued handler failed: %s", e)
        emit_audit(bus, action="consume.prescription.issued", actor="system:med-reconciliation-service",
                   target=None, details={"envelope_id": envelope.get("id")})

