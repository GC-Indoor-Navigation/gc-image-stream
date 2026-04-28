import httpx


JPEG_SOI = b"\xff\xd8"
JPEG_EOI = b"\xff\xd9"


def extract_mjpeg_frames(buffer: bytearray) -> list[bytes]:
    frames: list[bytes] = []

    while True:
        start_index = buffer.find(JPEG_SOI)
        if start_index == -1:
            buffer.clear()
            break

        end_index = buffer.find(JPEG_EOI, start_index + len(JPEG_SOI))
        if end_index == -1:
            if start_index > 0:
                del buffer[:start_index]
            break

        frame_end = end_index + len(JPEG_EOI)
        frames.append(bytes(buffer[start_index:frame_end]))
        del buffer[:frame_end]

    return frames


def iter_mjpeg_frames(
    session: httpx.Client,
    url: str,
    timeout_sec: float,
    chunk_size: int = 65_536,
):
    buffer = bytearray()
    with session.stream("GET", url, timeout=timeout_sec) as response:
        response.raise_for_status()

        for chunk in response.iter_bytes(chunk_size=chunk_size):
            if not chunk:
                continue

            buffer.extend(chunk)
            for frame in extract_mjpeg_frames(buffer):
                yield frame
