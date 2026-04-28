from camera.collector.config import CollectorConfig

from app.services.stream.recorder import (
    ExperimentContext,
    ExperimentRecorder,
    build_default_experiment_id,
    close_experiment_recorder,
    sanitize_experiment_id,
    start_generic_experiment_recorder,
)


def build_legacy_experiment_context(
    config: CollectorConfig,
    collector_type: str,
) -> ExperimentContext:
    return ExperimentContext(
        collector_type=collector_type,
        run_name=config.camera_name,
        experiment_log_dir=config.experiment_log_dir,
        experiment_id=config.experiment_id,
        summary_fields={
            "camera_name": config.camera_name,
            "source_url": config.source_url,
            "collect_interval_sec": config.collect_interval_sec,
            "legacy_register_api_url": config.legacy_register_api_url,
            "legacy_grpc_relay_target": config.legacy_grpc_relay_target,
            "legacy_storage_dir": config.legacy_storage_dir,
        },
    )


def start_experiment_recorder(
    config: CollectorConfig,
    collector_type: str,
) -> ExperimentRecorder | None:
    context = build_legacy_experiment_context(config, collector_type)
    return start_generic_experiment_recorder(context)


__all__ = [
    "ExperimentContext",
    "ExperimentRecorder",
    "build_default_experiment_id",
    "build_legacy_experiment_context",
    "close_experiment_recorder",
    "sanitize_experiment_id",
    "start_experiment_recorder",
    "start_generic_experiment_recorder",
]
