#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Storebælt Kamera-Überwachung (Gate + Pixel-Kalibrierung + AIS-Check + Repo-Alert)

Ablauf:
- ffmpeg zieht Snapshot aus M3U8
- Gate-Detektion: Frame-Diff nur innerhalb ROI-Polygone ("left_gate"/"right_gate")
- Objekt-Kandidat über Konturen + minAreaRect:
    - Mindestfläche
    - Mindest-Seitenverhältnis (elongated)
    - Mindestlänge in Pixel (aus Kalibrierung; entspricht >= 50m)
    - optional Tripline-Crossing (bei langsamen Polls eher soft)
- Wenn "großes Schiff im Gate" UND kein AIS-Ziel im Fence im Zeitfenster:
    - Snapshot lokal + ins Repo public/alerts/storebaelt/<YYYY-MM-DD>/
    - public/live_alerts_belt.geojson aktualisieren
    - Rotation + git add/commit/push
    - ntfy
"""

from __future__ import annotations

import os, json, time, shutil, subprocess, math, argparse
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
import urllib.request

import numpy as np
import cv2
from PIL import Image

# =========================
# DEINE BASIS-PFADE (wie gehabt)
# =========================
DATA_ROOT = Path(r"C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild")
LOGS_V4  = DATA_ROOT / "logs_v4"

REPO_ROOT   = Path(r"C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild_repo")
REPO_PUBLIC = REPO_ROOT / "public"

LOCAL_SNAP_ROOT = DATA_ROOT / "snapshots"
REPO_ALERTS_DIR = REPO_PUBLIC / "alerts" / "storebaelt"
LIVE_ALERTS_FP  = REPO_PUBLIC / "live_alerts_belt.geojson"

RAW_BASE = "https://raw.githubusercontent.com/mowlsint/AIS-Lagebild/main/public"

# =========================
# KAMERA / AIS (wie gehabt)
# =========================
CAMERA_NAME = "Storebaelt-EastPylon"
BRIDGE = "Storebælt"
SIDE = "unknown"

M3U8_URL = "https://stream.sob.m-dn.net/live/sb1/vKVhWPO2ysiYNGrNfA+Krw1/stream.m3u8?plain=true"

CAM_LAT = 55.3385
CAM_LON = 11.0350

AIS_FENCE = (10.90, 55.27, 11.20, 55.42)  # (min_lon, min_lat, max_lon, max_lat)
AIS_WINDOW_MIN = 10
TAIL_BYTES = 12 * 1024 * 1024

# =========================
# DETEKTIONS-KONFIG (kommt aus JSON, Defaults hier)
# =========================
CONFIG_PATH = DATA_ROOT / "storebaelt_gate_config.json"

DEFAULT_CFG = {
    # ROI Polygone in NORMALIZED coords [0..1]
    "rois": {
        "left_gate":  [[0.30, 0.58], [0.45, 0.58], [0.47, 0.78], [0.28, 0.78]],
        "right_gate": [[0.52, 0.58], [0.72, 0.58], [0.74, 0.78], [0.54, 0.78]],
    },
    "tripline_y": 0.70,             # normalized y (0..1)

    # Kalibrierung
    "px_per_meter": None,           # wird durch --calibrate gesetzt
    "min_len_meters": 50.0,
    "min_len_px": None,             # px_per_meter * min_len_meters

    # Snapshot-Polling
    "poll_seconds": 8,              # 8s ist i.d.R. stabiler als 15s für "Crossing"
    "cooldown_min": 10,

    # Bilddifferenz / Konturen
    "diff_pixel_threshold": 22,      # Empfindlichkeit (0..255)
    "min_contour_area": 900,         # in Pixeln (nach Maskierung)
    "min_aspect_ratio": 1.7,         # elongated blob
    "confirm_frames": 2,             # Kandidat muss in >=2 aufeinanderfolgenden Frames passen
    "track_max_age": 6,              # Frames bis Track verworfen
    "match_dist_px": 110,            # Track-Matching

    # Debug
    "debug_window": True,
    "save_debug_overlay": False,
}

# Repo-Rotation
KEEP_DAYS = 5


# =========================
# NOTIFY
# =========================
def send_ntfy(title: str, message: str) -> None:
    topic = (os.environ.get("ALERT_NTFY_TOPIC") or "aislagebild-bruecken").strip()
    server = (os.environ.get("ALERT_NTFY_SERVER") or "https://ntfy.sh").strip().rstrip("/")
    url = f"{server}/{topic}"
    try:
        req = urllib.request.Request(
            url,
            data=message.encode("utf-8"),
            headers={
                "Title": title,
                "Priority": "4",
                "Tags": "ship,alert",
                "Content-Type": "text/plain; charset=utf-8",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        print(f"[notify] ntfy failed: {e}")


# =========================
# TIME / AIS HELPERS
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def parse_ts_utc(ts: str) -> datetime | None:
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def in_bbox(lat: float, lon: float, bbox) -> bool:
    min_lon, min_lat, max_lon, max_lat = bbox
    return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

def newest_log_file(log_dir: Path) -> Path | None:
    files = sorted(log_dir.glob("bbox_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def tail_lines_jsonl(path: Path, tail_bytes: int) -> list[dict]:
    with open(path, "rb") as f:
        try:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - tail_bytes), os.SEEK_SET)
        except Exception:
            f.seek(0)
        data = f.read()

    lines = data.splitlines()
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln.decode("utf-8", errors="ignore")))
        except Exception:
            continue
    return out

def ais_any_target_in_fence(window_min: int) -> bool:
    fp = newest_log_file(LOGS_V4)
    if not fp:
        return False

    now = utcnow()
    start = now - timedelta(minutes=window_min)

    events = tail_lines_jsonl(fp, TAIL_BYTES)
    for ev in reversed(events):
        ts = ev.get("ts_utc")
        if not ts:
            continue
        dt = parse_ts_utc(ts)
        if not dt:
            continue
        if dt < start:
            break

        lat = ev.get("lat"); lon = ev.get("lon")
        if lat is None or lon is None:
            continue
        try:
            lat = float(lat); lon = float(lon)
        except Exception:
            continue

        if in_bbox(lat, lon, AIS_FENCE):
            return True
    return False


# =========================
# ffmpeg Snapshot
# =========================
def ffmpeg_snapshot(m3u8_url: str, out_jpg: Path) -> bool:
    out_jpg.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-i", m3u8_url,
        "-frames:v", "1",
        "-q:v", "2",
        str(out_jpg),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
        return r.returncode == 0 and out_jpg.exists() and out_jpg.stat().st_size > 10_000
    except Exception:
        return False


# =========================
# CONFIG LOAD/SAVE
# =========================
def load_cfg() -> dict:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CFG, indent=2, ensure_ascii=False), encoding="utf-8")
        return json.loads(json.dumps(DEFAULT_CFG))
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return json.loads(json.dumps(DEFAULT_CFG))

def save_cfg(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")


# =========================
# ROI MASK (normalized polygons -> pixel mask)
# =========================
def norm_poly_to_pts(poly_norm, w: int, h: int) -> np.ndarray:
    pts = []
    for x, y in poly_norm:
        pts.append([int(round(x * w)), int(round(y * h))])
    return np.array(pts, dtype=np.int32)

def make_gate_mask(cfg: dict, w: int, h: int) -> np.ndarray:
    mask = np.zeros((h, w), dtype=np.uint8)
    for _, poly_norm in cfg["rois"].items():
        pts = norm_poly_to_pts(poly_norm, w, h)
        cv2.fillPoly(mask, [pts], 255)
    return mask


# =========================
# SIMPLE TRACKER
# =========================
class Track:
    def __init__(self, tid: int, cx: int, cy: int, length_px: float, frame_idx: int):
        self.tid = tid
        self.cx = cx
        self.cy = cy
        self.prev_cy = cy
        self.length_px = length_px
        self.hits = 1
        self.last_seen = frame_idx
        self.crossed = False

def dist(a, b) -> float:
    return math.hypot(a[0]-b[0], a[1]-b[1])

def update_tracks(tracks: dict[int, Track], detections: list[tuple[int,int,float]], frame_idx: int, cfg: dict) -> dict[int, Track]:
    # age out
    max_age = int(cfg["track_max_age"])
    for tid in list(tracks.keys()):
        if frame_idx - tracks[tid].last_seen > max_age:
            del tracks[tid]

    match_dist = float(cfg["match_dist_px"])

    used = set()
    for (cx, cy, ln) in detections:
        best_tid = None
        best_d = 1e9
        for tid, tr in tracks.items():
            if tid in used:
                continue
            d = dist((cx,cy), (tr.cx, tr.cy))
            if d < best_d:
                best_d = d
                best_tid = tid

        if best_tid is not None and best_d <= match_dist:
            tr = tracks[best_tid]
            tr.prev_cy = tr.cy
            tr.cx, tr.cy = cx, cy
            tr.length_px = ln
            tr.hits += 1
            tr.last_seen = frame_idx
            used.add(best_tid)
        else:
            new_id = (max(tracks.keys()) + 1) if tracks else 1
            tracks[new_id] = Track(new_id, cx, cy, ln, frame_idx)

    return tracks


# =========================
# DETECTION (Gate + FrameDiff)
# =========================
def detect_big_ship(prev_bgr: np.ndarray, cur_bgr: np.ndarray, cfg: dict) -> tuple[bool, np.ndarray, list[tuple[int,int,float,tuple]]]:
    """
    returns:
      - found_big (bool)
      - debug_overlay (bgr)
      - detections: list of (cx,cy,length_px, box_pts)
    """
    h, w = cur_bgr.shape[:2]
    mask = make_gate_mask(cfg, w, h)

    # preprocess
    prev_g = cv2.cvtColor(prev_bgr, cv2.COLOR_BGR2GRAY)
    cur_g  = cv2.cvtColor(cur_bgr,  cv2.COLOR_BGR2GRAY)

    # contrast help (fog/compression)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    prev_g = clahe.apply(prev_g)
    cur_g  = clahe.apply(cur_g)

    prev_g = cv2.bitwise_and(prev_g, prev_g, mask=mask)
    cur_g  = cv2.bitwise_and(cur_g,  cur_g,  mask=mask)

    diff = cv2.absdiff(cur_g, prev_g)
    _, th = cv2.threshold(diff, int(cfg["diff_pixel_threshold"]), 255, cv2.THRESH_BINARY)

    # clean noise
    th = cv2.medianBlur(th, 5)
    kernel = np.ones((5,5), np.uint8)
    th = cv2.morphologyEx(th, cv2.MORPH_OPEN, kernel, iterations=1)
    th = cv2.morphologyEx(th, cv2.MORPH_DILATE, kernel, iterations=2)

    contours, _ = cv2.findContours(th, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    min_area = int(cfg["min_contour_area"])
    min_ar   = float(cfg["min_aspect_ratio"])

    min_len_px = cfg.get("min_len_px")
    min_len_px = float(min_len_px) if min_len_px is not None else None

    detections = []
    for c in contours:
        area = cv2.contourArea(c)
        if area < min_area:
            continue

        rect = cv2.minAreaRect(c)  # ((cx,cy),(rw,rh),angle)
        (cx, cy), (rw, rh), ang = rect
        if rw < 2 or rh < 2:
            continue

        length_px = float(max(rw, rh))
        short_px  = float(min(rw, rh))
        ar = length_px / (short_px + 1e-6)
        if ar < min_ar:
            continue

        if min_len_px is not None and length_px < min_len_px:
            continue

        box = cv2.boxPoints(rect)
        box = np.intp(box)

        detections.append((int(cx), int(cy), float(length_px), box))

    # build overlay
    overlay = cur_bgr.copy()
    # draw rois
    for name, poly_norm in cfg["rois"].items():
        pts = norm_poly_to_pts(poly_norm, w, h)
        cv2.polylines(overlay, [pts], True, (0,255,255), 2)
        cv2.putText(overlay, name, (pts[0][0], max(25, pts[0][1]-10)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0,255,255), 2)

    trip_y = int(round(float(cfg["tripline_y"]) * h))
    cv2.line(overlay, (0, trip_y), (w, trip_y), (255,0,0), 2)

    for (cx,cy,ln,box) in detections:
        cv2.drawContours(overlay, [box], 0, (0,255,0), 2)
        cv2.circle(overlay, (cx,cy), 6, (0,255,0), -1)
        cv2.putText(overlay, f"len~{ln:.0f}px", (cx+10, cy-10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0,255,0), 2)

    found_big = len(detections) > 0
    return found_big, overlay, detections


# =========================
# LIVE ALERTS + REPO
# =========================
def ensure_live_alerts_exists():
    LIVE_ALERTS_FP.parent.mkdir(parents=True, exist_ok=True)
    if not LIVE_ALERTS_FP.exists():
        LIVE_ALERTS_FP.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")

def add_alert_feature(ts: datetime, snapshot_rel_public: str):
    ensure_live_alerts_exists()
    try:
        fc = json.loads(LIVE_ALERTS_FP.read_text(encoding="utf-8"))
    except Exception:
        fc = {"type":"FeatureCollection","features":[]}

    props = {
        "camera_name": CAMERA_NAME,
        "bridge": BRIDGE,
        "side": SIDE,
        "timestamp_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshot_url": f"{RAW_BASE}/{snapshot_rel_public}",
    }

    feat = {
        "type": "Feature",
        "properties": props,
        "geometry": {"type":"Point","coordinates":[CAM_LON, CAM_LAT]},
    }

    fc["features"] = [feat] + (fc.get("features") or [])
    fc["features"] = fc["features"][:200]
    LIVE_ALERTS_FP.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")

def cleanup_repo_alerts_keep_days(keep_days: int):
    if not REPO_ALERTS_DIR.exists():
        return
    today = date.today()
    for child in REPO_ALERTS_DIR.iterdir():
        if not child.is_dir():
            continue
        try:
            d = datetime.strptime(child.name, "%Y-%m-%d").date()
        except Exception:
            continue
        if (today - d).days > keep_days:
            shutil.rmtree(child, ignore_errors=True)

def git_publish():
    def run(cmd):
        return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    run(["git","pull","--rebase"])
    run(["git","add","public/alerts","public/live_alerts_belt.geojson"])
    run(["git","commit","-m", f"Update camera alerts {utcnow().strftime('%Y-%m-%d %H:%MZ')}"])
    run(["git","push"])


# =========================
# UI: ROI Picker + Calibration
# =========================
def roi_pick(image_path: Path):
    img = cv2.imread(str(image_path))
    if img is None:
        raise SystemExit("Bild konnte nicht geladen werden.")
    h, w = img.shape[:2]
    cfg = load_cfg()

    stages = ["left_gate", "right_gate", "tripline"]
    stage_idx = 0
    current_pts = []
    tripline_y = None

    win = "ROI Picker (ENTER=next, ESC=cancel)"
    cv2.namedWindow(win, cv2.WINDOW_NORMAL)

    def draw():
        vis = img.copy()

        # already saved polys (if any)
        for name, poly_norm in cfg["rois"].items():
            pts = norm_poly_to_pts(poly_norm, w, h)
            cv2.polylines(vis, [pts], True, (0,255,255), 2)
            cv2.putText(vis, name, (pts[0][0], max(25, pts[0][1]-10)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0,255,255), 2)

        # current points
        if stages[stage_idx] in ("left_gate","right_gate"):
            for p in current_pts:
                cv2.circle(vis, p, 5, (0,255,0), -1)
            if len(current_pts) >= 2:
                cv2.polylines(vis, [np.array(current_pts, np.int32)], False, (0,255,0), 2)

        if stages[stage_idx] == "tripline" and tripline_y is not None:
            cv2.line(vis, (0, tripline_y), (w, tripline_y), (255,0,0), 2)

        txt = f"Stage: {stages[stage_idx]} | Click points (polygon) / click y (tripline). ENTER=save stage."
        cv2.putText(vis, txt, (20, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (255,255,255), 2)
        return vis

    def on_mouse(event, x, y, flags, param):
        nonlocal tripline_y
        if event == cv2.EVENT_LBUTTONDOWN:
            if stages[stage_idx] in ("left_gate","right_gate"):
                current_pts.append((x,y))
            else:
                tripline_y = y

    cv2.setMouseCallback(win, on_mouse)

    while True:
        cv2.imshow(win, draw())
        k = cv2.waitKey(20) & 0xFF
        if k == 27:
            cv2.destroyAllWindows()
            raise SystemExit("ROI-Picking abgebrochen.")
        if k in (10, 13):  # ENTER
            stage = stages[stage_idx]
            if stage in ("left_gate","right_gate"):
                if len(current_pts) < 3:
                    print("Mind. 3 Punkte fürs Polygon klicken.")
                    continue
                cfg["rois"][stage] = [[p[0]/w, p[1]/h] for p in current_pts]
                current_pts = []
                stage_idx += 1
            else:
                if tripline_y is None:
                    print("Tripline: einmal ins Bild klicken (y).")
                    continue
                cfg["tripline_y"] = float(tripline_y / h)
                stage_idx += 1

            if stage_idx >= len(stages):
                break

    cv2.destroyAllWindows()
    save_cfg(cfg)
    print(f"[cfg] gespeichert: {CONFIG_PATH}")

def calibrate(image_path: Path, ship_length_m: float):
    img = cv2.imread(str(image_path))
    if img is None:
        raise SystemExit("Bild konnte nicht geladen werden.")
    cfg = load_cfg()

    pts = []
    win = "Calibrate: Click BOW then STERN (2 clicks) | ESC=cancel"
    cv2.namedWindow(win, cv2.WINDOW_NORMAL)

    def on_mouse(event, x, y, flags, param):
        if event == cv2.EVENT_LBUTTONDOWN and len(pts) < 2:
            pts.append((x,y))

    cv2.setMouseCallback(win, on_mouse)

    while True:
        vis = img.copy()
        if len(pts) >= 1:
            cv2.circle(vis, pts[0], 6, (0,255,0), -1)
        if len(pts) == 2:
            cv2.circle(vis, pts[1], 6, (0,255,0), -1)
            cv2.line(vis, pts[0], pts[1], (0,255,0), 2)
        cv2.putText(vis, "Click bow then stern. 2 clicks total.", (20,30),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255,255,255), 2)
        cv2.imshow(win, vis)

        k = cv2.waitKey(20) & 0xFF
        if k == 27:
            cv2.destroyAllWindows()
            raise SystemExit("Kalibrierung abgebrochen.")
        if len(pts) == 2:
            break

    cv2.destroyAllWindows()

    px_len = math.hypot(pts[1][0] - pts[0][0], pts[1][1] - pts[0][1])
    px_per_meter = px_len / float(ship_length_m)

    cfg["px_per_meter"] = px_per_meter
    cfg["min_len_px"] = px_per_meter * float(cfg["min_len_meters"])

    save_cfg(cfg)

    print("[cal] done")
    print(f"  clicked_px_length = {px_len:.2f}px")
    print(f"  ship_length_m     = {ship_length_m:.2f}m")
    print(f"  px_per_meter      = {px_per_meter:.5f}")
    print(f"  min_len_meters    = {cfg['min_len_meters']:.2f}m")
    print(f"  min_len_px        = {cfg['min_len_px']:.2f}px")


# =========================
# MAIN LOOP
# =========================
def main_run():
    ensure_live_alerts_exists()
    cfg = load_cfg()

    poll_s = float(cfg["poll_seconds"])
    cooldown = timedelta(minutes=float(cfg["cooldown_min"]))

    tmp_dir = DATA_ROOT / "_tmp_cam"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    prev_fp = tmp_dir / "prev.jpg"
    cur_fp  = tmp_dir / "cur.jpg"

    tracks: dict[int, Track] = {}
    frame_idx = 0

    last_alert = datetime(1970,1,1,tzinfo=timezone.utc)

    print("[cam] starting Storebælt watcher (gate mode). Ctrl+C to stop.")
    send_ntfy("TEST", "camera_watch_storebaelt_gate.py gestartet ✅")

    while True:
        t0 = utcnow()

        ok = ffmpeg_snapshot(M3U8_URL, cur_fp)
        if not ok:
            print("[cam] snapshot failed")
            time.sleep(poll_s)
            continue

        if not prev_fp.exists():
            shutil.copy2(cur_fp, prev_fp)
            time.sleep(poll_s)
            continue

        # load frames
        prev_img = cv2.imread(str(prev_fp))
        cur_img  = cv2.imread(str(cur_fp))
        if prev_img is None or cur_img is None:
            shutil.copy2(cur_fp, prev_fp)
            time.sleep(poll_s)
            continue

        found_big, overlay, detections = detect_big_ship(prev_img, cur_img, cfg)

        # move forward
        shutil.copy2(cur_fp, prev_fp)

        # cooldown check early
        if (t0 - last_alert) < cooldown:
            if cfg.get("debug_window", True):
                cv2.imshow("overlay", overlay)
                if cv2.waitKey(1) & 0xFF == 27:
                    break
            time.sleep(poll_s)
            frame_idx += 1
            continue

        if not found_big:
            if cfg.get("debug_window", True):
                cv2.imshow("overlay", overlay)
                if cv2.waitKey(1) & 0xFF == 27:
                    break
            time.sleep(poll_s)
            frame_idx += 1
            continue

        # tracking / confirm
        det_simple = [(cx,cy,ln) for (cx,cy,ln,box) in detections]
        tracks = update_tracks(tracks, det_simple, frame_idx, cfg)

        trip_y = int(round(float(cfg["tripline_y"]) * cur_img.shape[0]))
        confirm_frames = int(cfg["confirm_frames"])

        big_confirmed = False
        for tr in tracks.values():
            # crossing (soft): previous above line and current below
            if tr.prev_cy < trip_y and tr.cy >= trip_y:
                tr.crossed = True
            # falls Poll sehr langsam ist: akzeptiere auch "nahe Tripline"
            near_line = abs(tr.cy - trip_y) <= int(0.03 * cur_img.shape[0])

            if tr.hits >= confirm_frames and (tr.crossed or near_line):
                big_confirmed = True
                break

        if not big_confirmed:
            if cfg.get("debug_window", True):
                cv2.imshow("overlay", overlay)
                if cv2.waitKey(1) & 0xFF == 27:
                    break
            time.sleep(poll_s)
            frame_idx += 1
            continue

        # AIS check
        has_ais = ais_any_target_in_fence(AIS_WINDOW_MIN)
        if has_ais:
            time.sleep(poll_s)
            frame_idx += 1
            continue

        # => ALERT
        day = t0.strftime("%Y-%m-%d")
        ts_tag = t0.strftime("%Y%m%d_%H%M%S")

        local_dir = LOCAL_SNAP_ROOT / "storebaelt" / day
        local_dir.mkdir(parents=True, exist_ok=True)
        local_fp = local_dir / f"{ts_tag}_{CAMERA_NAME}.jpg"

        shutil.copy2(prev_fp, local_fp)

        repo_day_dir = REPO_ALERTS_DIR / day
        repo_day_dir.mkdir(parents=True, exist_ok=True)
        repo_fp = repo_day_dir / local_fp.name
        shutil.copy2(local_fp, repo_fp)

        snapshot_rel_public = f"alerts/storebaelt/{day}/{repo_fp.name}"
        add_alert_feature(t0, snapshot_rel_public)

        cleanup_repo_alerts_keep_days(KEEP_DAYS)
        git_publish()

        snapshot_url = f"{RAW_BASE}/{snapshot_rel_public}"
        title = f"ALERT {BRIDGE} {SIDE}"
        text = (
            f"Camera: {CAMERA_NAME}\n"
            f"UTC: {t0.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"Snapshot: {snapshot_url}\n"
            f"Gate: YES | Size>=50m: {'YES' if cfg.get('min_len_px') else 'UNKNOWN'}\n"
        )
        send_ntfy(title, text)

        if cfg.get("save_debug_overlay", False):
            dbg_dir = DATA_ROOT / "snapshots" / "storebaelt_debug" / day
            dbg_dir.mkdir(parents=True, exist_ok=True)
            cv2.imwrite(str(dbg_dir / f"{ts_tag}_overlay.jpg"), overlay)

        last_alert = t0
        print(f"[cam] ALERT saved (no AIS, big ship in gate): {repo_fp.name}")

        time.sleep(poll_s)
        frame_idx += 1

    cv2.destroyAllWindows()


# =========================
# CLI
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--roi-pick", action="store_true")
    ap.add_argument("--calibrate", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--image", type=str, default=None)
    ap.add_argument("--ship-length-m", type=float, default=152.0)
    args = ap.parse_args()

    if args.roi_pick:
        if not args.image:
            raise SystemExit("--roi-pick braucht --image")
        roi_pick(Path(args.image))
        return

    if args.calibrate:
        if not args.image:
            raise SystemExit("--calibrate braucht --image")
        calibrate(Path(args.image), args.ship_length_m)
        return

    if args.run:
        main_run()
        return

    # default: create cfg if missing and show help
    _ = load_cfg()
    print(f"Config: {CONFIG_PATH}")
    ap.print_help()

if __name__ == "__main__":
    main()
