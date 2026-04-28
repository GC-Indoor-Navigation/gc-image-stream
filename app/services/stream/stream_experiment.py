from app.services.stream.experiment_recorder import (
    ExperimentContext,
    ExperimentRecorder,
    close_experiment_recorder,
    start_generic_experiment_recorder,
)


stream_experiment_recorder: ExperimentRecorder | None = None


def configure_stream_experiment_recorder(
    experiment_log_dir: str,
    experiment_id: str,
    storage_dir: str,
    relay_target: str,
    camera_ids: list[str] | None = None,
):
    global stream_experiment_recorder
    clear_stream_experiment_recorder()

    context = ExperimentContext(
        collector_type="stream_server",
        run_name="stream-server",
        experiment_log_dir=experiment_log_dir,
        experiment_id=experiment_id or None,
        summary_fields={
            "camera_name": "stream-server",
            "camera_ids": list(camera_ids or []),
            "storage_dir": storage_dir,
            "processing_relay_target": relay_target or None,
        },
    )
    stream_experiment_recorder = start_generic_experiment_recorder(context)
    return stream_experiment_recorder


def get_stream_experiment_recorder() -> ExperimentRecorder | None:
    return stream_experiment_recorder


def clear_stream_experiment_recorder():
    global stream_experiment_recorder
    close_experiment_recorder(stream_experiment_recorder)
    stream_experiment_recorder = None
