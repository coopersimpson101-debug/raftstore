"""Regenerate the README figures from real runs (stdlib only, deterministic).

    python scripts/make_figures.py

Writes, under docs/assets/:
  banner.svg        project banner; the motif on the right is the trace below, in miniature
  sim_timeline.svg  node roles, faults and commit index for one seeded simulator schedule
  chaos_demo.svg    docs/chaos_demo_output.txt (a real chaos_demo.py run) as a terminal window

The simulator run uses the "real-WAL-on-faulty-disk+drive-pulls" scenario from
tests/test_simulation.py with seed 42; safety invariants are checked after every step, and the
script fails if the history is not linearizable or an acknowledged write was lost.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from xml.sax.saxutils import escape

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from raftstore.raft.types import CANDIDATE, LEADER, PRE_CANDIDATE  # noqa: E402
from raftstore.sim.cluster import SimCluster, SimConfig  # noqa: E402

ASSETS = os.path.join(ROOT, "docs", "assets")
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
BG, FG, MUTED, GRID = "#0d1117", "#e6edf3", "#8b949e", "#21262d"
ACCENT = "#2DD4BF"
ROLE_COLOUR = {"L": ACCENT, "C": "#d29922", "P": "#9e6a03", "F": "#30363d", "D": None}
NODE_COLOUR = ["#2DD4BF", "#58a6ff", "#d2a8ff", "#f0883e", "#7ee787"]
SCENARIO = "real-WAL-on-faulty-disk+drive-pulls"


# --------------------------------------------------------------------------- trace
class TraceCluster(SimCluster):
    """SimCluster that records every node's role and commit index after every step."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.samples: list[dict] = []
        self.heal_start: int | None = None

    def step(self) -> None:
        super().step()
        roles, commits = {}, {}
        for nid, n in self.nodes.items():
            if not n.alive:
                roles[nid], commits[nid] = "D", None
                continue
            st = n.raft.state
            roles[nid] = {LEADER: "L", CANDIDATE: "C", PRE_CANDIDATE: "P"}.get(st, "F")
            commits[nid] = n.raft.log.commit
        self.samples.append({"t": self.now, "roles": roles, "commit": commits,
                             "blocked": bool(self.net.blocked)})


def run_trace(seed: int = 42, steps: int = 1200) -> TraceCluster:
    c = TraceCluster(seed, SimConfig(storage="disk", p_pull_drive=0.003))
    c.run(steps)
    c.heal_start = c.now
    c.heal()
    res = c.check_linearizable()
    if not res.ok:
        raise SystemExit(f"seed {seed}: history not linearizable (key {res.key!r})")
    c.acked_verified = c.check_no_acked_write_lost()
    return c


def events(c: TraceCluster) -> list[tuple[int, str, str]]:
    """(step, node, kind) for crashes, restarts and drive pulls, parsed from the simulator log."""
    out = []
    for line in c.events:
        m = re.match(r"\[t=\s*(\d+)\] (n\d+)(:? )(.*)", line)
        if not m:
            continue
        t, nid, rest = int(m.group(1)), m.group(2), m.group(4)
        if rest.startswith("crashed"):
            out.append((t, nid, "crash"))
        elif rest.startswith("restarted"):
            out.append((t, nid, "restart"))
        elif rest.startswith("drive pulled"):
            out.append((t, nid, "pull"))
    return out


def runs(values):
    """Run-length encode a sequence: [(start, end_exclusive, value)]."""
    out, start = [], 0
    for i in range(1, len(values) + 1):
        if i == len(values) or values[i] != values[start]:
            out.append((start, i, values[start]))
            start = i
    return out


# ------------------------------------------------------------------------- helpers
def grid_defs(pid: str, size: int = 32) -> str:
    return (f'<pattern id="{pid}" width="{size}" height="{size}" patternUnits="userSpaceOnUse">'
            f'<path d="M {size} 0 L 0 0 0 {size}" fill="none" stroke="{GRID}" stroke-width="1"/></pattern>')


def hatch_defs(pid: str) -> str:
    return (f'<pattern id="{pid}" width="6" height="6" patternUnits="userSpaceOnUse" '
            f'patternTransform="rotate(45)"><rect width="6" height="6" fill="#1c1215"/>'
            f'<line x1="0" y1="0" x2="0" y2="6" stroke="#f85149" stroke-width="1.6" opacity="0.55"/></pattern>')


def lanes_svg(c, ids, x0, w, y0, lane_h, gap, hatch_id, n_steps=None):
    s = c.samples if n_steps is None else c.samples[:n_steps]
    sx = w / len(s)
    parts = []
    for li, nid in enumerate(ids):
        y = y0 + li * (lane_h + gap)
        for a, b, r in runs([smp["roles"][nid] for smp in s]):
            fill = ROLE_COLOUR[r] or f"url(#{hatch_id})"
            parts.append(f'<rect x="{x0 + a * sx:.1f}" y="{y}" width="{max((b - a) * sx, 0.6):.1f}" '
                         f'height="{lane_h}" fill="{fill}"/>')
    return parts, sx


# ------------------------------------------------------------------------ timeline
def timeline_svg(c: TraceCluster, seed: int) -> str:
    W, H = 1200, 540
    x0, x1 = 96, 1172
    w = x1 - x0
    ids = sorted(c.nodes)
    lane_h, gap, ly = 30, 12, 112
    s = c.samples
    n = len(s)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
           f'font-family="{MONO}">',
           f'<defs>{grid_defs("g", 40)}{hatch_defs("down")}</defs>',
           f'<rect width="{W}" height="{H}" rx="12" fill="{BG}"/>',
           f'<rect width="{W}" height="{H}" rx="12" fill="url(#g)" opacity="0.5"/>',
           f'<text x="24" y="38" font-size="18" font-weight="700" fill="{FG}">One simulated schedule, '
           f'step by step</text>',
           f'<text x="24" y="62" font-size="12.5" fill="{MUTED}">scenario {escape(SCENARIO)} · seed {seed} · '
           f'3 nodes, real WAL on a fault-injecting disk, lossy/reordering network</text>']
    # legend
    lx = 760
    for label, col in (("leader", ROLE_COLOUR["L"]), ("candidate", ROLE_COLOUR["C"]),
                       ("pre-vote", ROLE_COLOUR["P"]), ("follower", ROLE_COLOUR["F"])):
        out.append(f'<rect x="{lx}" y="27" width="12" height="12" rx="2" fill="{col}"/>'
                   f'<text x="{lx + 17}" y="37" font-size="11.5" fill="{MUTED}">{label}</text>')
        lx += 30 + len(label) * 7.2
    out.append(f'<rect x="{lx}" y="27" width="12" height="12" rx="2" fill="url(#down)"/>'
               f'<text x="{lx + 17}" y="37" font-size="11.5" fill="{MUTED}">down</text>')
    sx = w / n
    lanes_bottom = ly + len(ids) * (lane_h + gap) - gap
    # partitions + heal phase (behind lanes)
    for a, b, v in runs([smp["blocked"] for smp in s]):
        if v:
            out.append(f'<rect x="{x0 + a * sx:.1f}" y="{ly - 10}" width="{(b - a) * sx:.1f}" '
                       f'height="{lanes_bottom - ly + 20}" fill="#a371f7" opacity="0.22"/>')
    hx = x0 + (c.heal_start or n) * sx
    out.append(f'<line x1="{hx:.1f}" y1="{ly - 18}" x2="{hx:.1f}" y2="{H - 70}" stroke="{MUTED}" '
               f'stroke-dasharray="4 4"/>')
    out.append(f'<text x="{hx - 6:.1f}" y="{ly - 20}" font-size="11" text-anchor="end" fill="{MUTED}">'
               f'heal phase: faults off, converge →</text>')
    out.append(f'<text x="{x0}" y="{ly - 20}" font-size="11" fill="#a371f7">▮ network partition</text>')
    parts, _ = lanes_svg(c, ids, x0, w, ly, lane_h, gap, "down")
    out += parts
    for li, nid in enumerate(ids):
        y = ly + li * (lane_h + gap)
        out.append(f'<text x="{x0 - 14}" y="{y + lane_h / 2 + 5}" font-size="13" font-weight="700" '
                   f'text-anchor="end" fill="{NODE_COLOUR[li]}">{nid}</text>')
    # fault markers
    for t, nid, kind in events(c):
        if nid not in ids:
            continue
        li = ids.index(nid)
        x = x0 + (t - 1) * sx
        y = ly + li * (lane_h + gap)
        if kind == "crash":
            out.append(f'<path d="M{x - 5:.1f},{y - 6} l10,10 m0,-10 l-10,10" stroke="#f85149" '
                       f'stroke-width="2.4" stroke-linecap="round"/>')
        elif kind == "pull":
            out.append(f'<path d="M{x:.1f},{y + lane_h + 1} l5,7 l-10,0 z" fill="#f0883e"/>')
    # commit index chart
    cy0, cy1 = lanes_bottom + 50, H - 78
    maxc = max(v for smp in s for v in smp["commit"].values() if v is not None) or 1
    out.append(f'<text x="24" y="{cy0 - 14}" font-size="12.5" fill="{FG}" font-weight="700">commit index'
               f'<tspan fill="{MUTED}" font-weight="400"> per node (gaps = node down; jumps = catch-up '
               f'after restart or partition)</tspan></text>')
    for k in range(5):
        v = round(maxc * k / 4)
        y = cy1 - (cy1 - cy0) * k / 4
        out.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" stroke="{GRID}"/>'
                   f'<text x="{x0 - 10}" y="{y + 4:.1f}" font-size="10.5" text-anchor="end" '
                   f'fill="{MUTED}">{v}</text>')
    for li, nid in enumerate(ids):
        segs, cur = [], []
        for i, smp in enumerate(s):
            v = smp["commit"][nid]
            if v is None:
                if cur:
                    segs.append(cur)
                cur = []
                continue
            cur.append(f"{x0 + i * sx:.1f},{cy1 - (cy1 - cy0) * v / maxc:.1f}")
        if cur:
            segs.append(cur)
        for seg in segs:
            out.append(f'<polyline points="{" ".join(seg)}" fill="none" stroke="{NODE_COLOUR[li]}" '
                       f'stroke-width="1.8" stroke-linejoin="round" opacity="0.9"/>')
    # x axis
    for t in range(0, n + 1, 200):
        x = x0 + t * sx
        out.append(f'<line x1="{x:.1f}" y1="{cy1}" x2="{x:.1f}" y2="{cy1 + 5}" stroke="{MUTED}"/>'
                   f'<text x="{x:.1f}" y="{cy1 + 18}" font-size="10.5" text-anchor="middle" '
                   f'fill="{MUTED}">{t}</text>')
    out.append(f'<text x="{(x0 + x1) / 2}" y="{cy1 + 34}" font-size="10.5" text-anchor="middle" '
               f'fill="{MUTED}">simulated step →</text>')
    st = c.summary()
    foot = (f'{st["steps"]:,} steps · {st["invariant_checks"]:,} invariant checks · '
            f'{c.stats["crashes"]} crashes · {c.stats["partitions"]} partitions · '
            f'{c.stats["drive_pulls"]} drive pulls · {st["msgs_dropped"]:,} msgs dropped · '
            f'{st["ops"]} client ops → linearizable ✓ · {c.acked_verified} acked writes all present ✓')
    out.append(f'<text x="24" y="{H - 24}" font-size="12" fill="{ACCENT}">{escape(foot)}</text>')
    out.append(f'<text x="{x0 + 170}" y="{ly - 20}" font-size="11" fill="{MUTED}">'
               f'<tspan fill="#f85149">✕</tspan> crash (kill -9 / power cut)   '
               f'<tspan fill="#f0883e">▲</tspan> drive pulled</text>')
    out.append("</svg>")
    return "\n".join(out)


# -------------------------------------------------------------------------- banner
def banner_svg(c: TraceCluster) -> str:
    W, H = 1280, 400
    ids = sorted(c.nodes)
    x0, w = 720, 520
    lane_h, gap, ly = 22, 10, 120
    parts, sx = lanes_svg(c, ids, x0, w, ly, lane_h, gap, "bdown")
    s = c.samples
    cy0, cy1 = 230, 320
    maxc = max(v for smp in s for v in smp["commit"].values() if v is not None) or 1
    lines = []
    for li, nid in enumerate(ids):
        pts, segs = [], []
        for i, smp in enumerate(s):
            v = smp["commit"][nid]
            if v is None:
                if pts:
                    segs.append(pts)
                pts = []
                continue
            pts.append(f"{x0 + i * sx:.1f},{cy1 - (cy1 - cy0) * v / maxc:.1f}")
        if pts:
            segs.append(pts)
        for seg in segs:
            lines.append(f'<polyline points="{" ".join(seg)}" fill="none" stroke="{ACCENT}" '
                         f'stroke-width="2" opacity="{0.95 - 0.25 * li:.2f}"/>')
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
           f'font-family="{MONO}">',
           '<defs>', grid_defs("bg", 32), hatch_defs("bdown"),
           '<linearGradient id="fade" x1="0" x2="1" y1="0" y2="0">'
           '<stop offset="0" stop-color="#fff" stop-opacity="0"/>'
           '<stop offset="0.25" stop-color="#fff" stop-opacity="1"/></linearGradient>'
           f'<mask id="m"><rect x="{x0}" y="0" width="{w}" height="{H}" fill="url(#fade)"/></mask>',
           '<radialGradient id="glow" cx="0.78" cy="0.5" r="0.5">'
           f'<stop offset="0" stop-color="{ACCENT}" stop-opacity="0.10"/>'
           f'<stop offset="1" stop-color="{ACCENT}" stop-opacity="0"/></radialGradient>',
           '</defs>',
           f'<rect width="{W}" height="{H}" fill="{BG}"/>',
           f'<rect width="{W}" height="{H}" fill="url(#bg)" opacity="0.6"/>',
           f'<rect width="{W}" height="{H}" fill="url(#glow)"/>',
           f'<rect x="72" y="118" width="6" height="74" fill="{ACCENT}"/>',
           f'<text x="98" y="178" font-size="76" font-weight="700" fill="{FG}">raftstore</text>',
           f'<text x="100" y="232" font-size="22" fill="{MUTED}">Self-healing replicated storage on a '
           f'from-scratch Raft</text>',
           f'<text x="100" y="272" font-size="15" fill="{ACCENT}">stdlib-only Python · deterministic '
           f'simulation · linearizability-checked</text>',
           '<g mask="url(#m)">', *parts, *lines, '</g>',
           f'<text x="{x0 + w}" y="{ly - 14}" font-size="12" text-anchor="end" fill="{MUTED}">'
           f'real simulator trace · seed 42 · roles + commit index</text>',
           "</svg>"]
    return "\n".join(out)


# ------------------------------------------------------------------------ terminal
def terminal_svg(lines: list[str], title: str) -> str:
    fs, lh, pad = 13, 19.5, 22
    W = 1180
    H = int(64 + pad + lh * len(lines) + pad)
    hot = re.compile(r"(PULLING THE DRIVE|BIT ROT|REPLACING|kill -9|fail-stopped|exit code 75)")
    good = re.compile(r"(ALL PRESENT|byte-identical|True|DONE|linearizable|converged)")
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
           f'font-family="{MONO}" font-size="{fs}">',
           f'<rect x="0.5" y="0.5" width="{W - 1}" height="{H - 1}" rx="10" fill="{BG}" stroke="#30363d"/>',
           f'<path d="M0.5,40 h{W - 1}" stroke="#30363d"/>',
           '<circle cx="22" cy="20" r="6" fill="#ff5f56"/><circle cx="42" cy="20" r="6" fill="#ffbd2e"/>'
           '<circle cx="62" cy="20" r="6" fill="#27c93f"/>',
           f'<text x="{W / 2}" y="25" text-anchor="middle" fill="{MUTED}" font-size="12.5">'
           f'{escape(title)}</text>',
           f'<text x="{pad}" y="{64 + 4}" fill="{ACCENT}">$ <tspan fill="{FG}">python scripts/chaos_demo.py'
           f'</tspan></text>']
    y = 64 + pad + 6
    for line in lines:
        m = re.match(r"(\[\s*[\d.]+s\])(.*)", line)
        ts, rest = (m.group(1), m.group(2)) if m else ("", line)
        spans = []
        pos = 0
        for mm in sorted(list(hot.finditer(rest)) + list(good.finditer(rest)), key=lambda x: x.start()):
            if mm.start() < pos:
                continue
            spans.append((rest[pos:mm.start()], FG))
            spans.append((mm.group(0), "#f0883e" if hot.fullmatch(mm.group(0)) else ACCENT))
            pos = mm.end()
        spans.append((rest[pos:], FG if ts else MUTED))
        body = "".join(f'<tspan fill="{col}">{escape(t)}</tspan>' for t, col in spans if t)
        out.append(f'<text x="{pad}" y="{y:.1f}" xml:space="preserve"><tspan fill="{MUTED}">'
                   f'{escape(ts)}</tspan>{body}</text>')
        y += lh
    out.append("</svg>")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--steps", type=int, default=1200)
    a = ap.parse_args()
    os.makedirs(ASSETS, exist_ok=True)
    c = run_trace(a.seed, a.steps)
    files = {"sim_timeline.svg": timeline_svg(c, a.seed), "banner.svg": banner_svg(c)}
    with open(os.path.join(ROOT, "docs", "chaos_demo_output.txt"), encoding="utf-8") as f:
        demo = [ln.rstrip("\n") for ln in f if ln.strip()]
    files["chaos_demo.svg"] = terminal_svg(demo, "chaos_demo.py — 3 real processes on localhost")
    for name, svg in files.items():
        with open(os.path.join(ASSETS, name), "w", encoding="utf-8", newline="\n") as f:
            f.write(svg + "\n")
        print(f"wrote docs/assets/{name} ({len(svg) // 1024} KiB)")
    st = c.summary()
    print(f"seed {a.seed}: {st['steps']} steps, {c.stats['crashes']} crashes, {c.stats['partitions']} partitions, "
          f"{c.stats['drive_pulls']} drive pulls, {st['ops']} ops linearizable, {c.acked_verified} acked writes verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
