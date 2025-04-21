import json
import os
import sys

import pytest

import ray


@pytest.mark.asyncio
async def test_task_labels(tmp_path):
    """
    Test task events are correctly generated and written to file
    """
    os.environ["RAY_enable_export_api_write"] = "1"
    ray.init(_temp_dir=str(tmp_path))

    @ray.remote
    def hi_w00t_task():
        return 1

    ray.get(hi_w00t_task.options(_labels={"hi": "w00t"}).remote())
    # Shutdown the cluster to ensure all events are flushed
    ray.shutdown()

    export_event_path = os.path.join(
        str(tmp_path), "session_latest", "logs", "export_events"
    )
    # Verify export events are written
    events = []
    for filename in os.listdir(export_event_path):
        if not filename.startswith("event_EXPORT_TASK"):
            continue
        with open(f"{export_event_path}/{filename}", "r") as f:
            for line in f.readlines():
                events.append(json.loads(line))

    hi_w00t_event = next(
        (
            event
            for event in events
            if event["source_type"] == "EXPORT_TASK"
            and event["event_data"].get("task_info", {}).get("func_or_class_name")
            == "hi_w00t_task"
        ),
        None,
    )
    assert (
        hi_w00t_event is not None
    ), f"Event for task 'hi_w00t_task' not found in {events}"
    assert (
        hi_w00t_event["event_data"].get("task_info", {}).get("labels", {}).get("hi")
        == "w00t"
    ), f"Label 'hi':'w00t' not found for task 'hi_w00t_task': {hi_w00t_event}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
