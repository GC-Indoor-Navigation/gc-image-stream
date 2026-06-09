def parse_phone_alert_subscriptions(raw_value: str | None) -> dict[str, tuple[str, ...]]:
    if raw_value is None or not raw_value.strip():
        return {}

    subscriptions: dict[str, tuple[str, ...]] = {}
    entries = [
        entry.strip()
        for entry in raw_value.split(";")
        if entry.strip()
    ]
    for entry in entries:
        separator = "=" if "=" in entry else ":"
        if separator not in entry:
            raise RuntimeError(
                "PHONE_ALERT_SUBSCRIPTIONS entries must use phone_id=camera1,camera2",
            )
        phone_id, camera_list = entry.split(separator, 1)
        phone_id = phone_id.strip()
        camera_ids = tuple(
            dict.fromkeys(
                item.strip()
                for item in camera_list.split(",")
                if item.strip()
            )
        )
        if not phone_id or not camera_ids:
            raise RuntimeError(
                "PHONE_ALERT_SUBSCRIPTIONS entries require a phone id and at least one camera id",
            )
        subscriptions[phone_id] = camera_ids
    return subscriptions


def resolve_phone_alert_camera_device_ids(
    *,
    phone_device_ids: list[str],
    explicit_camera_device_ids: list[str] | None = None,
    subscription_mapping: dict[str, tuple[str, ...]] | None = None,
) -> list[str]:
    camera_ids: list[str] = []
    for camera_id in explicit_camera_device_ids or []:
        if camera_id and camera_id not in camera_ids:
            camera_ids.append(camera_id)

    mapping = subscription_mapping or {}
    for phone_id in phone_device_ids:
        for camera_id in mapping.get(phone_id, ()):
            if camera_id not in camera_ids:
                camera_ids.append(camera_id)

    if camera_ids:
        return camera_ids
    return list(dict.fromkeys(phone_device_ids))
