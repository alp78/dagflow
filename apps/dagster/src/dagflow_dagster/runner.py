from __future__ import annotations

import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date
from threading import Lock
from typing import Any

from dagflow_dagster.definitions import defs
from dagflow_dagster.execution import export_step_key
from dagflow_dagster.resources import ControlPlaneResource, build_resources

_LOAD_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="dagflow-load")
_ACTIVE_LOADS: dict[tuple[str, str], dict[str, Any]] = {}
_ACTIVE_LOADS_LOCK = Lock()
_EXPORT_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="dagflow-export")
_ACTIVE_EXPORTS: dict[str, Future[Any]] = {}
_ACTIVE_EXPORTS_LOCK = Lock()
WORKSPACE_PIPELINE_ORDER = ("security_master", "shareholder_holdings")


def deterministic_run_id(pipeline_code: str, business_date: date) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"https://dagflow.local/operational/{pipeline_code}/{business_date.isoformat()}",
    )


def _job_name_for_pipeline(pipeline_code: str, *, export: bool = False) -> str:
    if export:
        return f"{pipeline_code}_export_job"
    return f"{pipeline_code}_job"


def _control_plane(resources: dict[str, Any]) -> ControlPlaneResource:
    resource = resources["control_plane"]
    if not isinstance(resource, ControlPlaneResource):
        raise RuntimeError("Dagster control_plane resource is not configured correctly.")
    return resource


def _ensure_available_date(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    business_date: date,
) -> None:
    available_dates = control_plane.list_available_source_dates(pipeline_code)
    for entry in available_dates:
        if entry["business_date"] == business_date.isoformat() and bool(entry["is_ready"]):
            return
    raise RuntimeError(
        f"{pipeline_code} has no ready landed sources for {business_date.isoformat()}."
    )


def _workspace_loaded_state(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
) -> list[dict[str, Any]]:
    loaded_states: list[dict[str, Any]] = []
    for required_pipeline_code in _required_workspace_pipelines(pipeline_code):
        state = control_plane.current_pipeline_state(required_pipeline_code)
        if state is None:
            continue
        if state.get("last_run_id") is None or state.get("last_business_date") is None:
            continue
        loaded_states.append(
            {
                "pipeline_code": required_pipeline_code,
                "dataset_code": state.get("dataset_code") or required_pipeline_code,
                "run_id": str(state["last_run_id"]),
                "business_date": str(state["last_business_date"]),
            }
        )
    return loaded_states


def _assert_workspace_available(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
) -> None:
    loaded_states = _workspace_loaded_state(control_plane, pipeline_code)
    if not loaded_states:
        return
    loaded_dates = sorted({str(state["business_date"]) for state in loaded_states})
    date_label = ", ".join(loaded_dates)
    raise RuntimeError(
        "The review workbench already has loaded operational data for "
        f"{date_label}. Reset the workbench before loading another day."
    )


def _pipeline_is_current_for_date(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    business_date: date,
) -> bool:
    state = control_plane.current_pipeline_state(pipeline_code)
    if state is None:
        return False
    return state.get("last_business_date") == business_date and state.get("last_run_id") is not None


def _planned_pipeline_runs(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    business_date: date,
) -> list[dict[str, Any]]:
    planned_runs: list[dict[str, Any]] = []
    relations = _workspace_relations(pipeline_code)
    for required_pipeline_code in _required_workspace_pipelines(pipeline_code):
        _ensure_available_date(control_plane, required_pipeline_code, business_date)
        planned_runs.append(
            {
                "pipeline_code": required_pipeline_code,
                "dataset_code": required_pipeline_code,
                "run_id": str(deterministic_run_id(required_pipeline_code, business_date)),
                "business_date": business_date.isoformat(),
                "relation": relations.get(required_pipeline_code, "primary"),
                "status": "queued",
            }
        )
    return planned_runs


def _workspace_relations(pipeline_code: str) -> dict[str, str]:
    return {
        "security_master": "primary" if pipeline_code == "security_master" else "prerequisite",
        "shareholder_holdings": (
            "primary" if pipeline_code == "shareholder_holdings" else "companion"
        ),
    }


def _required_workspace_pipelines(pipeline_code: str) -> tuple[str, ...]:
    if pipeline_code in WORKSPACE_PIPELINE_ORDER:
        return WORKSPACE_PIPELINE_ORDER
    return (pipeline_code,)


def _current_workspace_runs(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    business_date: date,
) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    relations = _workspace_relations(pipeline_code)
    for required_pipeline_code in _required_workspace_pipelines(pipeline_code):
        state = control_plane.current_pipeline_state(required_pipeline_code)
        if state is None:
            return []
        run_id = state.get("last_run_id")
        if run_id is None or state.get("last_business_date") != business_date:
            return []
        runs.append(
            {
                "pipeline_code": required_pipeline_code,
                "dataset_code": state.get("dataset_code") or required_pipeline_code,
                "run_id": str(run_id),
                "business_date": business_date.isoformat(),
                "relation": relations.get(required_pipeline_code, "primary"),
                "status": (
                    control_plane.pipeline_run_status(uuid.UUID(str(run_id)))
                    or "pending_review"
                ),
            }
        )
    return runs


def _prepare_planned_runs(
    control_plane: ControlPlaneResource,
    planned_runs: list[dict[str, Any]],
) -> None:
    for planned_run in planned_runs:
        control_plane.prepare_pipeline_run(
            pipeline_code=str(planned_run["pipeline_code"]),
            dataset_code=str(planned_run["dataset_code"]),
            run_id=uuid.UUID(str(planned_run["run_id"])),
            business_date=date.fromisoformat(str(planned_run["business_date"])),
            orchestrator_run_id=None,
            status="queued",
            metadata={"launch_relation": planned_run["relation"]},
        )


def _load_key(pipeline_code: str, business_date: date) -> tuple[str, str]:
    return pipeline_code, business_date.isoformat()


def _finalize_planned_runs_on_failure(
    planned_runs: list[dict[str, Any]],
    error: Exception,
) -> None:
    resources = build_resources()
    control_plane = _control_plane(resources)
    for planned_run in planned_runs:
        run_id = uuid.UUID(str(planned_run["run_id"]))
        status = control_plane.pipeline_run_status(run_id)
        if status not in {"queued", "running"}:
            continue
        control_plane.block_pipeline_run(
            run_id=run_id,
            status="blocked" if planned_run["relation"] == "primary" else "failed",
            metadata={
                "launcher_error_class": error.__class__.__name__,
                "launcher_error_message": str(error),
            },
        )


def _execute_launched_pipeline_load(
    business_date: date,
    planned_runs: list[dict[str, Any]],
    load_keys: list[tuple[str, str]],
) -> None:
    try:
        if not planned_runs:
            return
        for planned_run in planned_runs:
            run_pipeline_for_date(
                str(planned_run["pipeline_code"]),
                business_date,
                workspace_prepared=True,
            )
    except Exception as error:
        _finalize_planned_runs_on_failure(planned_runs, error)
        raise
    finally:
        with _ACTIVE_LOADS_LOCK:
            for load_key in load_keys:
                _ACTIVE_LOADS.pop(load_key, None)


def launch_pipeline_for_date(
    pipeline_code: str,
    business_date: date,
) -> dict[str, Any]:
    resources = build_resources()
    control_plane = _control_plane(resources)
    _ensure_available_date(control_plane, pipeline_code, business_date)
    planned_runs = _planned_pipeline_runs(control_plane, pipeline_code, business_date)
    key = _load_key(pipeline_code, business_date)
    requested_run_id = str(deterministic_run_id(pipeline_code, business_date))
    load_keys = [_load_key(str(run["pipeline_code"]), business_date) for run in planned_runs]

    with _ACTIVE_LOADS_LOCK:
        active = _ACTIVE_LOADS.get(key)
        active_future = active.get("future") if active is not None else None
        if isinstance(active_future, Future) and not active_future.done():
            return {
                "requested_pipeline_code": pipeline_code,
                "dataset_code": pipeline_code,
                "business_date": business_date.isoformat(),
                "requested_run_id": requested_run_id,
                "run_ids": [str(run["run_id"]) for run in active["planned_runs"]],
                "runs": active["planned_runs"],
                "already_running": True,
            }

        if planned_runs:
            _assert_workspace_available(control_plane, pipeline_code)
            _prepare_planned_runs(control_plane, planned_runs)
            future = _LOAD_EXECUTOR.submit(
                _execute_launched_pipeline_load,
                business_date,
                planned_runs,
                load_keys,
            )
            active_payload = {"future": future, "planned_runs": planned_runs}
            for load_key in load_keys:
                _ACTIVE_LOADS[load_key] = active_payload

    return {
        "requested_pipeline_code": pipeline_code,
        "dataset_code": pipeline_code,
        "business_date": business_date.isoformat(),
        "requested_run_id": requested_run_id,
        "run_ids": [str(run["run_id"]) for run in planned_runs],
        "runs": planned_runs,
        "already_running": False,
    }


def _export_run_payload(
    pipeline_code: str,
    run_id: uuid.UUID,
    business_date: date,
    *,
    status: str = "approved",
    already_running: bool = False,
) -> dict[str, Any]:
    return {
        "requested_pipeline_code": pipeline_code,
        "dataset_code": pipeline_code,
        "business_date": business_date.isoformat(),
        "requested_run_id": str(run_id),
        "run_ids": [str(run_id)],
        "runs": [
            {
                "pipeline_code": pipeline_code,
                "dataset_code": pipeline_code,
                "run_id": str(run_id),
                "business_date": business_date.isoformat(),
                "relation": "primary",
                "status": status,
                "steps": [],
            }
        ],
        "already_running": already_running,
    }


def _workspace_export_payload(
    requested_pipeline_code: str,
    business_date: date,
    runs: list[dict[str, Any]],
    *,
    already_running: bool = False,
) -> dict[str, Any]:
    fallback_run_id = str(deterministic_run_id(requested_pipeline_code, business_date))
    requested_run_id = next(
        (
            str(run["run_id"])
            for run in runs
            if str(run["pipeline_code"]) == requested_pipeline_code
        ),
        str(runs[0]["run_id"]) if runs else fallback_run_id,
    )
    return {
        "requested_pipeline_code": requested_pipeline_code,
        "dataset_code": requested_pipeline_code,
        "business_date": business_date.isoformat(),
        "requested_run_id": requested_run_id,
        "run_ids": [str(run["run_id"]) for run in runs],
        "runs": runs,
        "already_running": already_running,
    }


def _execute_launched_export(
    pipeline_code: str,
    run_id: uuid.UUID,
    business_date: date,
) -> None:
    try:
        run_export_for_run(pipeline_code, run_id, business_date)
    finally:
        with _ACTIVE_EXPORTS_LOCK:
            _ACTIVE_EXPORTS.pop(str(run_id), None)


def launch_export_for_run(
    pipeline_code: str,
    run_id: uuid.UUID,
    business_date: date,
) -> dict[str, Any]:
    resources = build_resources()
    control_plane = _control_plane(resources)
    run_status = control_plane.pipeline_run_status(run_id)
    if run_status == "exported":
        return _export_run_payload(
            pipeline_code,
            run_id,
            business_date,
            status="exported",
        )

    with _ACTIVE_EXPORTS_LOCK:
        active_future = _ACTIVE_EXPORTS.get(str(run_id))
        if active_future is not None and not active_future.done():
            return _export_run_payload(
                pipeline_code,
                run_id,
                business_date,
                status="approved",
                already_running=True,
            )

        control_plane.start_pipeline_step(
            pipeline_code=pipeline_code,
            dataset_code=pipeline_code,
            run_id=run_id,
            business_date=business_date,
            step_key=export_step_key(pipeline_code),
            metadata={"stage": "validated_export", "trigger": "review_approval"},
            mark_run_running=False,
        )
        future = _EXPORT_EXECUTOR.submit(
            _execute_launched_export,
            pipeline_code,
            run_id,
            business_date,
        )
        _ACTIVE_EXPORTS[str(run_id)] = future

    return _export_run_payload(
        pipeline_code,
        run_id,
        business_date,
        status="approved",
    )


def launch_workspace_export(
    requested_pipeline_code: str,
    business_date: date,
) -> dict[str, Any]:
    resources = build_resources()
    control_plane = _control_plane(resources)
    relations = _workspace_relations(requested_pipeline_code)
    runs: list[dict[str, Any]] = []
    exports_already_running = False

    with _ACTIVE_EXPORTS_LOCK:
        for pipeline_code in _required_workspace_pipelines(requested_pipeline_code):
            state = control_plane.current_pipeline_state(pipeline_code)
            if (
                state is None
                or state.get("last_run_id") is None
                or state.get("last_business_date") != business_date
            ):
                raise RuntimeError(
                    "The full workspace must be loaded for the selected business "
                    "date before export can start."
                )

            run_id = uuid.UUID(str(state["last_run_id"]))
            run_status = control_plane.pipeline_run_status(run_id) or str(
                state.get("approval_state") or "approved"
            )
            run_payload = {
                "pipeline_code": pipeline_code,
                "dataset_code": state.get("dataset_code") or pipeline_code,
                "run_id": str(run_id),
                "business_date": business_date.isoformat(),
                "relation": relations.get(pipeline_code, "primary"),
                "status": run_status,
                "steps": [],
            }

            if run_status == "exported":
                run_payload["status"] = "exported"
                runs.append(run_payload)
                continue

            active_future = _ACTIVE_EXPORTS.get(str(run_id))
            if active_future is not None and not active_future.done():
                run_payload["status"] = "approved"
                runs.append(run_payload)
                exports_already_running = True
                continue

            control_plane.start_pipeline_step(
                pipeline_code=pipeline_code,
                dataset_code=str(state.get("dataset_code") or pipeline_code),
                run_id=run_id,
                business_date=business_date,
                step_key=export_step_key(pipeline_code),
                metadata={
                    "stage": "validated_export",
                    "trigger": "workspace_review_approval",
                },
                mark_run_running=False,
            )
            future = _EXPORT_EXECUTOR.submit(
                _execute_launched_export,
                pipeline_code,
                run_id,
                business_date,
            )
            _ACTIVE_EXPORTS[str(run_id)] = future
            run_payload["status"] = "approved"
            runs.append(run_payload)

    return _workspace_export_payload(
        requested_pipeline_code,
        business_date,
        runs,
        already_running=exports_already_running,
    )


def run_pipeline_for_date(
    pipeline_code: str,
    business_date: date,
    *,
    workspace_prepared: bool = False,
) -> dict[str, Any]:
    resources = build_resources()
    control_plane = _control_plane(resources)
    _ensure_available_date(control_plane, pipeline_code, business_date)

    if not workspace_prepared and pipeline_code in WORKSPACE_PIPELINE_ORDER:
        _assert_workspace_available(control_plane, pipeline_code)
        for required_pipeline_code in _required_workspace_pipelines(pipeline_code):
            run_pipeline_for_date(
                required_pipeline_code,
                business_date,
                workspace_prepared=True,
            )
        return {
            "requested_pipeline_code": pipeline_code,
            "dataset_code": pipeline_code,
            "business_date": business_date.isoformat(),
            "requested_run_id": str(deterministic_run_id(pipeline_code, business_date)),
            "run_ids": [
                str(deterministic_run_id(required_pipeline_code, business_date))
                for required_pipeline_code in _required_workspace_pipelines(pipeline_code)
            ],
            "runs": [
                {
                    "pipeline_code": required_pipeline_code,
                    "dataset_code": required_pipeline_code,
                    "run_id": str(deterministic_run_id(required_pipeline_code, business_date)),
                    "business_date": business_date.isoformat(),
                    "relation": _workspace_relations(pipeline_code).get(
                        required_pipeline_code,
                        "primary",
                    ),
                    "status": "pending_review",
                }
                for required_pipeline_code in _required_workspace_pipelines(pipeline_code)
            ],
            "status": "pending_review",
        }

    if pipeline_code == "shareholder_holdings":
        security_state = control_plane.current_pipeline_state("security_master")
        if security_state is None or security_state.get("last_business_date") != business_date:
            run_pipeline_for_date(
                "security_master",
                business_date,
                workspace_prepared=workspace_prepared,
            )

    run_id = deterministic_run_id(pipeline_code, business_date)
    control_plane.prepare_pipeline_run(
        pipeline_code=pipeline_code,
        dataset_code=pipeline_code,
        run_id=run_id,
        business_date=business_date,
        orchestrator_run_id=None,
        status="queued",
    )
    job = defs.get_job_def(_job_name_for_pipeline(pipeline_code))
    result = job.execute_in_process(
        resources=resources,
        tags={
            "pipeline_code": pipeline_code,
            "dagflow_pipeline_code": pipeline_code,
            "dagflow_run_id": str(run_id),
            "business_date": business_date.isoformat(),
            "dagflow_business_date": business_date.isoformat(),
        },
    )
    if not result.success:
        raise RuntimeError(
            f"Dagster job for {pipeline_code} on {business_date.isoformat()} failed."
        )
    if pipeline_code == "security_master":
        control_plane.relink_current_holdings_security_reviews(business_date)
    return {
        "pipeline_code": pipeline_code,
        "dataset_code": pipeline_code,
        "run_id": str(run_id),
        "business_date": business_date.isoformat(),
        "status": "pending_review",
    }


def run_export_for_run(
    pipeline_code: str,
    run_id: uuid.UUID,
    business_date: date,
) -> dict[str, Any]:
    resources = build_resources()
    job = defs.get_job_def(_job_name_for_pipeline(pipeline_code, export=True))
    result = job.execute_in_process(
        resources=resources,
        tags={
            "pipeline_code": pipeline_code,
            "dagflow_pipeline_code": pipeline_code,
            "dagflow_run_id": str(run_id),
            "business_date": business_date.isoformat(),
            "dagflow_business_date": business_date.isoformat(),
            "validated_business_date": business_date.isoformat(),
        },
    )
    if not result.success:
        raise RuntimeError(
            f"Dagster export job for {pipeline_code} on {business_date.isoformat()} failed."
        )
    return {
        "pipeline_code": pipeline_code,
        "run_id": str(run_id),
        "business_date": business_date.isoformat(),
        "status": "exported",
    }


def reset_workbench(
    *,
    actor: str = "ui",
    notes: str | None = None,
) -> dict[str, Any]:
    with _ACTIVE_LOADS_LOCK:
        active_loads = [
            key
            for key, payload in _ACTIVE_LOADS.items()
            if isinstance(payload.get("future"), Future) and not payload["future"].done()
        ]
    with _ACTIVE_EXPORTS_LOCK:
        active_exports = [
            run_id for run_id, future in _ACTIVE_EXPORTS.items() if not future.done()
        ]
    if active_loads or active_exports:
        raise RuntimeError(
            "The workbench cannot be reset while Dagster is still loading or exporting."
        )

    resources = build_resources()
    control_plane = _control_plane(resources)
    reset_payloads: list[dict[str, Any]] = []
    for pipeline_code in WORKSPACE_PIPELINE_ORDER:
        reset_payloads.append(
            control_plane.wipe_pipeline_workspace_for_load(
                pipeline_code=pipeline_code,
                dataset_code=pipeline_code,
                actor=actor,
                notes=notes
                or "Reset reviewer workbench and clear the current operational workspace",
            )
        )
    with _ACTIVE_LOADS_LOCK:
        _ACTIVE_LOADS.clear()
    with _ACTIVE_EXPORTS_LOCK:
        _ACTIVE_EXPORTS.clear()
    return {
        "workspace": "review",
        "pipelines": reset_payloads,
    }
