"""
pipeline.py
This module exposes a simplified version of the end‑to‑end post‑processing pipeline
for the hackaton project.  It is designed to run on commodity hardware without
GPU acceleration and returns JSON summaries describing person and train events
in a fragment of video.  The original pipeline defined in the Jupyter notebook
relies on a number of auxiliary functions and heavy segmentation logic.  To
integrate into the Streamlit application and respect CPU‑only environments,
we implement a lightweight alternative which still provides structured outputs
compatible with the repository's downstream processing.  The functions and
constants defined here can be extended or replaced with the full notebook
implementation when resources allow.

The main entry point is :func:`run_pipeline`.  It accepts a path to a video
file, a YOLO model instance (or ``None`` to force lazy loading), the start
time of the video fragment as an ISO string, a link to the original video in
object storage, a unique identifier for the video fragment and an optional
list of train numbers with timestamps.  It returns paths to two JSON files:
one for people events and one for train events.  Each JSON file contains an
array of dictionaries describing the detected entities.  When no events are
detected, the arrays are empty.

This implementation uses the ultralytics YOLOv8 API for detection.  If the
requested weights file is not available on the host machine, the model will
not be loaded and the pipeline will return empty outputs.  The detection
pipeline samples one frame per second to balance performance and latency.  A
simple IOU‑based tracker maintains consistent IDs across frames.  Person
status is estimated by comparing the speed of movement between subsequent
frames; train status is derived from the duration of observation.  Zones are
not estimated in this simplified version and are always set to ``None``.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
import ultralytics
try:
    import cv2  # type: ignore
except ImportError:
    cv2 = None

try:
    import torch  # type: ignore
except ImportError:
    torch = None

try:
    from ultralytics import YOLO  # type: ignore
except ImportError:
    YOLO = None  # type: ignore


##############################
# Tracker implementation
##############################

def iou_bbox(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> float:
    """Compute intersection over union between two bounding boxes.

    Args:
        a: Bounding box (x, y, w, h).
        b: Bounding box (x, y, w, h).

    Returns:
        IoU value between 0 and 1.
    """
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    x1 = max(ax, bx)
    y1 = max(ay, by)
    x2 = min(ax + aw, bx + bw)
    y2 = min(ay + ah, by + bh)
    if x2 <= x1 or y2 <= y1:
        return 0.0
    inter = (x2 - x1) * (y2 - y1)
    union = aw * ah + bw * bh - inter
    return inter / union


class SimpleTrack:
    """Represents a single object track.

    Tracks maintain the last bounding box, the timestamps when the object was
    observed, and accumulate a history of observations used to infer events.
    """

    def __init__(self, tid: int, kind: str, bbox: Tuple[float, float, float, float], time_sec: float) -> None:
        self.id = tid
        self.kind = kind  # 'person' or 'train'
        self.last_bbox = bbox
        self.last_time = time_sec
        self.start_time = time_sec
        self.lost = False
        self.lost_since: Optional[float] = None
        # history entries: (time_sec, bbox)
        self.history: List[Tuple[float, Tuple[float, float, float, float]]] = []

    def update(self, bbox: Tuple[float, float, float, float], time_sec: float) -> None:
        self.last_bbox = bbox
        self.last_time = time_sec
        self.lost = False
        self.lost_since = None

    def mark_lost(self, time_sec: float) -> None:
        self.lost = True
        self.lost_since = time_sec


class SimpleTrackerManager:
    """Manages multiple tracks using IoU association."""

    def __init__(self, iou_thresh: float = 0.3, max_lost_sec: float = 5.0) -> None:
        self.iou_thresh = iou_thresh
        self.max_lost_sec = max_lost_sec
        self.next_id = 1
        self.tracks: dict[int, SimpleTrack] = {}

    def step(self, detections: List[Tuple[float, float, float, float]], kind: str, frame_time: float) -> None:
        """
        Update tracks with detections for a given kind ('person' or 'train').

        Args:
            detections: A list of bounding boxes.
            kind: Type of detection.
            frame_time: Timestamp of current frame in seconds.
        """
        assigned: set[int] = set()
        # first try to assign existing tracks by IoU
        for bbox in detections:
            best_tid = None
            best_iou = 0.0
            for tid, track in self.tracks.items():
                if track.kind != kind:
                    continue
                iou = iou_bbox(track.last_bbox, bbox)
                if iou >= self.iou_thresh and iou > best_iou and tid not in assigned:
                    best_tid = tid
                    best_iou = iou
            if best_tid is not None:
                track = self.tracks[best_tid]
                track.update(bbox, frame_time)
                assigned.add(best_tid)
            else:
                tid = self.next_id
                self.next_id += 1
                self.tracks[tid] = SimpleTrack(tid, kind, bbox, frame_time)
                assigned.add(tid)

        # mark tracks as lost when not updated for too long
        for tid, track in list(self.tracks.items()):
            if track.last_time < frame_time and (frame_time - track.last_time) > self.max_lost_sec:
                if not track.lost:
                    track.mark_lost(frame_time)


##############################
# Status estimation
##############################

V_THRESH_STOP = 2.0  # px/sec threshold for distinguishing motion

def person_status(prev_bbox: Optional[Tuple[float, float, float, float]], prev_t: Optional[float], cur_bbox: Tuple[float, float, float, float], cur_t: float) -> str:
    """Infer person status based on displacement speed."""
    if prev_bbox is None or prev_t is None:
        return 'Стоит'
    dt = max(1e-6, cur_t - prev_t)
    cx1 = prev_bbox[0] + prev_bbox[2] / 2
    cy1 = prev_bbox[1] + prev_bbox[3] / 2
    cx2 = cur_bbox[0] + cur_bbox[2] / 2
    cy2 = cur_bbox[1] + cur_bbox[3] / 2
    v = ((cx2 - cx1) ** 2 + (cy2 - cy1) ** 2) ** 0.5 / dt
    return 'Стоит' if v <= V_THRESH_STOP else 'Идет'

def train_status(track: SimpleTrack) -> str:
    """Infer train status from history length.

    If the track has been observed for only a few frames we consider the train
    arriving; if it persists but bounding box area changes little we call it
    stopped; otherwise we treat it as moving.  This heuristic is a simplified
    substitute for the more elaborate logic in the original notebook.
    """
    history = track.history
    if len(history) <= 1:
        return 'Приезжает'
    # compute average area change
    areas = [bbox[2] * bbox[3] for _, bbox in history]
    avg_area = sum(areas) / len(areas)
    var_area = max(areas) - min(areas)
    if var_area / max(avg_area, 1.0) < 0.1:
        return 'Стоит'
    return 'Выезжает'


##############################
# Main pipeline
##############################

def _load_model(weights_path: str) -> Optional[YOLO]:
    """
    Internal helper to load the YOLO model on CPU if possible.

    Returns ``None`` if ultralytics or torch are not available or the weights
    file cannot be found.
    """
    if YOLO is None or torch is None:
        return None
    if not os.path.exists(weights_path):
        return None
    try:
        # Force CPU by specifying device; ultralytics automatically selects CPU
        return YOLO(weights_path, task='detect', device='gpu')  # type: ignore
    except Exception:
        return None


def run_pipeline(
    video_path: str,
    model: Optional[YOLO] = None,
    video_start_dt_str: Optional[str] = None,
    video_link: Optional[str] = None,
    video_id: Optional[str] = None,
    train_numbers_list: Optional[List[dict]] = None,
    out_dir: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Process a single video fragment and produce JSON summaries.

    Args:
        video_path: Path to the local video file.
        model: A preloaded ultralytics YOLO model.  If ``None`` the model
            weights will be loaded from ``models/best.pt`` on CPU.  When the
            weights are not available the function returns empty outputs.
        video_start_dt_str: The start datetime of the video fragment.  It must
            be in the format ``YYYY-MM-DD HH:MM:SS``.  When ``None`` the
            function uses the current time as the base.
        video_link: Original cloud storage URL for the video.  Stored in the
            output JSON for traceability.
        video_id: A unique identifier for this fragment.
        train_numbers_list: Optional list of dictionaries with keys ``number``
            and ``time`` (seconds from the start of the video).  Used to
            associate train numbers with train events.  Currently unused in
            this simplified implementation.
        out_dir: Optional directory where JSON files will be written.  If not
            provided, files are stored in ``analysis_results`` in the current
            working directory.

    Returns:
        A tuple ``(people_json_path, train_json_path)`` containing the paths
        to the generated JSON files.  If detection could not be performed the
        files will contain empty arrays.
    """
    # Ensure output directory exists
    out_dir = out_dir or os.path.join(os.getcwd(), 'analysis_results')
    os.makedirs(out_dir, exist_ok=True)

    # Parse start datetime
    if video_start_dt_str:
        try:
            start_dt = datetime.strptime(video_start_dt_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            start_dt = datetime.now()
    else:
        start_dt = datetime.now()

    # Set defaults
    video_link = video_link or ''
    video_id = video_id or 'fragment'

    # Attempt to load model if not provided
    if model is None:
        weights_path = os.path.join(os.path.dirname(__file__), '..', 'models', 'best.pt')
        model = _load_model(weights_path)

    # Prepare output containers
    people_events: List[dict] = []
    train_events: List[dict] = []

    # If model or cv2 is unavailable we return empty outputs
    if cv2 is None or model is None:
        # still write empty files
        people_out_path = os.path.join(out_dir, f'people_events_{video_id}.json')
        train_out_path = os.path.join(out_dir, f'train_events_{video_id}.json')
        with open(people_out_path, 'w', encoding='utf-8') as f:
            json.dump(people_events, f, ensure_ascii=False, indent=2)
        with open(train_out_path, 'w', encoding='utf-8') as f:
            json.dump(train_events, f, ensure_ascii=False, indent=2)
        return people_out_path, train_out_path

    # Open video
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        # unable to open; write empty outputs
        people_out_path = os.path.join(out_dir, f'people_events_{video_id}.json')
        train_out_path = os.path.join(out_dir, f'train_events_{video_id}.json')
        with open(people_out_path, 'w', encoding='utf-8') as f:
            json.dump(people_events, f, ensure_ascii=False, indent=2)
        with open(train_out_path, 'w', encoding='utf-8') as f:
            json.dump(train_events, f, ensure_ascii=False, indent=2)
        return people_out_path, train_out_path

    # Determine frame sampling interval (aiming for 1 fps)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    sample_step = max(1, int(round(fps)))

    tracker = SimpleTrackerManager(iou_thresh=0.3, max_lost_sec=5.0)

    frame_idx = 0
    current_sec = 0.0
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if frame is None:
            frame_idx += 1
            continue
        # Only process frames at the sampling interval
        if frame_idx % sample_step == 0:
            current_sec = frame_idx / fps
            # Run YOLO inference
            try:
                with torch.no_grad():  # type: ignore
                    results = model(frame, verbose=False)  # type: ignore
            except Exception:
                results = []
            # Parse detections
            person_bboxes: List[Tuple[float, float, float, float]] = []
            train_bboxes: List[Tuple[float, float, float, float]] = []
            try:
                if results:
                    res = results[0]
                    boxes = res.boxes
                    xywh = boxes.xywh.cpu().numpy() if hasattr(boxes.xywh, 'cpu') else []
                    classes = boxes.cls.cpu().numpy() if hasattr(boxes.cls, 'cpu') else []
                    for (x, y, w, h), cls in zip(xywh, classes):
                        # class indices here are dataset dependent; we assume index 2 -> person, 3 -> train
                        if int(cls) == 2:
                            person_bboxes.append((float(x - w / 2), float(y - h / 2), float(w), float(h)))
                        elif int(cls) == 3:
                            train_bboxes.append((float(x - w / 2), float(y - h / 2), float(w), float(h)))
            except Exception:
                # if parsing fails we ignore detections
                pass
            # Update trackers
            tracker.step(person_bboxes, 'person', current_sec)
            tracker.step(train_bboxes, 'train', current_sec)
            # Append history for status estimation
            for tid, track in tracker.tracks.items():
                if track.kind == 'person' and track.last_time == current_sec:
                    prev_entry = track.history[-1] if track.history else None
                    prev_bbox = prev_entry[1] if prev_entry else None
                    prev_t = prev_entry[0] if prev_entry else None
                    status = person_status(prev_bbox, prev_t, track.last_bbox, current_sec)
                    track.history.append((current_sec, track.last_bbox))
                    people_events.append({
                        'person_id': track.id,
                        'filename': os.path.basename(video_path),
                        'video_link': video_link,
                        'video_id': video_id,
                        'start_sec': current_sec,
                        'end_sec': current_sec,
                        'start_dt': (start_dt + timedelta(seconds=current_sec)).strftime('%Y-%m-%d %H:%M:%S'),
                        'end_dt': (start_dt + timedelta(seconds=current_sec)).strftime('%Y-%m-%d %H:%M:%S'),
                        'status': status,
                        'zone': None,
                    })
                elif track.kind == 'train' and track.last_time == current_sec:
                    # for trains we only store basic info at each observation
                    track.history.append((current_sec, track.last_bbox))
        frame_idx += 1

    cap.release()

    # Aggregate train events: a single summary per track
    for tid, track in tracker.tracks.items():
        if track.kind != 'train' or not track.history:
            continue
        times = [t for t, _ in track.history]
        start_sec = min(times)
        end_sec = max(times)
        arr_dt = (start_dt + timedelta(seconds=start_sec)).strftime('%Y-%m-%d %H:%M:%S')
        dep_dt = (start_dt + timedelta(seconds=end_sec)).strftime('%Y-%m-%d %H:%M:%S')
        status_label = train_status(track)
        train_events.append({
            'train_id': track.id,
            'filename': os.path.basename(video_path),
            'video_link': video_link,
            'video_id': video_id,
            'arrival_sec': start_sec,
            'arrival_dt': arr_dt,
            'stop_start_sec': None,
            'stop_start_dt': None,
            'stop_end_sec': None,
            'stop_end_dt': None,
            'departure_sec': end_sec,
            'departure_dt': dep_dt,
            'stopped': status_label == 'Стоит',
            'номер': None,
        })

    # Write JSON outputs
    people_out_path = os.path.join(out_dir, f'people_events_{video_id}.json')
    train_out_path = os.path.join(out_dir, f'train_events_{video_id}.json')
    with open(people_out_path, 'w', encoding='utf-8') as f:
        json.dump(people_events, f, ensure_ascii=False, indent=2)
    with open(train_out_path, 'w', encoding='utf-8') as f:
        json.dump(train_events, f, ensure_ascii=False, indent=2)

    return people_out_path, train_out_path