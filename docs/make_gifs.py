"""Render the README animations: code on the left, a client/server sequence diagram on the right.

uv run --with pillow python docs/make_gifs.py
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).parent
FONT_PATH = "/System/Library/Fonts/Menlo.ttc"

WIDTH = 900
CODE_X = 24
CODE_Y = 64
LINE_H = 24
CLIENT_X = 560
SERVER_X = 850
LANE_TOP = 52
FIRST_ARROW_Y = 92
ARROW_GAP = 30
CAPTION_H = 52

BG = "#0d1117"
PANEL = "#161b22"
TEXT = "#c9d1d9"
DIM = "#6e7681"
HIGHLIGHT = "#1c2d41"
HIGHLIGHT_BAR = "#388bfd"
REQUEST = "#58a6ff"
RESPONSE = "#3fb950"
CONTROL = "#8b949e"
ERROR = "#f85149"

MOVE_FRAMES = 8
MOVE_MS = 45
HOLD_MS = 1500
FINAL_MS = 3500

UP = "up"  # client -> server
DOWN = "down"  # server -> client


@dataclass
class Arrow:
    label: str
    direction: str
    color: str
    dashed: bool = False


@dataclass
class Step:
    line: int
    caption: str
    arrows: tuple[Arrow, ...] = ()


def req(label: str) -> Arrow:
    return Arrow(label, UP, REQUEST)


def resp(label: str) -> Arrow:
    return Arrow(label, DOWN, RESPONSE)


def ctl(label: str, direction: str, color: str = CONTROL) -> Arrow:
    return Arrow(label, direction, color, dashed=True)


font = ImageFont.truetype(FONT_PATH, 14)
small = ImageFont.truetype(FONT_PATH, 12)
title_font = ImageFont.truetype(FONT_PATH, 13)


def dashed_line(draw: ImageDraw.ImageDraw, x0: float, x1: float, y: float, color: str) -> None:
    step = 1 if x1 >= x0 else -1
    x = x0
    while (x1 - x) * step > 0:
        end = x + step * 6
        if (x1 - end) * step < 0:
            end = x1
        draw.line([(x, y), (end, y)], fill=color, width=2)
        x += step * 10


def draw_arrow(draw: ImageDraw.ImageDraw, arrow: Arrow, y: float, progress: float) -> None:
    start, end = (CLIENT_X, SERVER_X) if arrow.direction == UP else (SERVER_X, CLIENT_X)
    head = start + (end - start) * progress
    if arrow.dashed:
        dashed_line(draw, start, head, y, arrow.color)
    else:
        draw.line([(start, y), (head, y)], fill=arrow.color, width=2)
    sign = 1 if end > start else -1
    draw.polygon([(head, y), (head - sign * 9, y - 5), (head - sign * 9, y + 5)], fill=arrow.color)
    if progress >= 1:
        middle = (CLIENT_X + SERVER_X) / 2
        draw.text((middle, y - 6), arrow.label, font=small, fill=arrow.color, anchor="mb")


def render(
    title: str,
    code: list[str],
    step: Step,
    *,
    done: list[Arrow],
    moving: Arrow | None,
    progress: float,
    height: int,
) -> Image.Image:
    img = Image.new("RGB", (WIDTH, height), BG)
    draw = ImageDraw.Draw(img)
    caption_top = height - CAPTION_H

    draw.rounded_rectangle([12, 40, CLIENT_X - 90, caption_top - 12], radius=8, fill=PANEL)
    draw.text((CODE_X, 14), title, font=title_font, fill=DIM)

    y = CODE_Y + step.line * LINE_H
    draw.rectangle([12, y - 4, CLIENT_X - 90, y + LINE_H - 6], fill=HIGHLIGHT)
    draw.rectangle([12, y - 4, 15, y + LINE_H - 6], fill=HIGHLIGHT_BAR)
    for i, line in enumerate(code):
        color = TEXT if i == step.line else DIM if line.lstrip().startswith("#") else "#8b949e"
        draw.text((CODE_X, CODE_Y + i * LINE_H), line, font=font, fill=color)

    for x, name in ((CLIENT_X, "client"), (SERVER_X, "server")):
        draw.text((x, LANE_TOP - 8), name, font=title_font, fill=TEXT, anchor="mb")
        draw.line([(x, LANE_TOP), (x, caption_top - 12)], fill="#30363d", width=2)

    for i, arrow in enumerate(done):
        draw_arrow(draw, arrow, FIRST_ARROW_Y + i * ARROW_GAP, 1.0)
    if moving is not None:
        draw_arrow(draw, moving, FIRST_ARROW_Y + len(done) * ARROW_GAP, progress)

    draw.line([(12, caption_top), (WIDTH - 12, caption_top)], fill="#30363d")
    draw.text((CODE_X, caption_top + CAPTION_H / 2), step.caption, font=font, fill="#e6edf3", anchor="lm")
    return img


def make(name: str, title: str, code: list[str], steps: list[Step]) -> None:
    arrows = sum(len(s.arrows) for s in steps)
    body = max(CODE_Y + len(code) * LINE_H, FIRST_ARROW_Y + arrows * ARROW_GAP)
    height = body + 12 + CAPTION_H

    frames: list[Image.Image] = []
    durations: list[int] = []
    done: list[Arrow] = []
    for step in steps:
        for arrow in step.arrows:
            for f in range(1, MOVE_FRAMES + 1):
                frames.append(
                    render(
                        title, code, step, done=done, moving=arrow, progress=f / MOVE_FRAMES, height=height
                    )
                )
                durations.append(MOVE_MS)
            done.append(arrow)
        frames.append(render(title, code, step, done=done, moving=None, progress=0, height=height))
        durations.append(HOLD_MS)
    durations[-1] = FINAL_MS

    path = OUT / f"{name}.gif"
    paletted = [f.convert("P", palette=Image.Palette.ADAPTIVE, colors=32) for f in frames]
    paletted[0].save(
        path, save_all=True, append_images=paletted[1:], duration=durations, loop=0, optimize=True
    )
    print(f"{path.name}: {len(frames)} frames, {path.stat().st_size // 1024} KiB")


def bidi() -> None:
    code = [
        "with BidiStream(stub.Chat) as s:",
        '    s.write(Msg(value="ping"))',
        "    reply = s.read(timeout=5)",
        "",
        '    s.write(Msg(value="a"))',
        '    s.write(Msg(value="b"))',
        "    s.done_writing()",
        "    for m in s:",
        "        print(m.value)",
        "    s.code()",
    ]
    steps = [
        Step(0, "The call starts in the constructor.", (ctl("open stream", UP),)),
        Step(1, "write() queues the message and returns at once.", (req("ping"),)),
        Step(2, "read() waits for the next message (here up to 5 s).", (resp("PING"),)),
        Step(4, "write() never blocks: messages go out in the background.", (req("a"),)),
        Step(5, "write() never blocks: messages go out in the background.", (req("b"),)),
        Step(6, "done_writing() half-closes: no more requests.", (ctl("half-close", UP),)),
        Step(7, "Iteration reads messages until the stream ends...", (resp("A"), resp("B"))),
        Step(7, "...then stops: read() would return EOF.", (ctl("status OK", DOWN, RESPONSE),)),
        Step(9, "code(), details(), trailing_metadata() give the final status."),
    ]
    make("bidi", "BidiStream: stream-stream", code, steps)


def response() -> None:
    code = [
        "with ResponseStream(",
        '    stub.Expand, Msg(value="x"),',
        ") as s:",
        "    first = s.read(timeout=5)",
        "    for m in s:",
        "        print(m.value)",
        "    s.read()  # EOF",
    ]
    steps = [
        Step(1, "The request goes to the constructor; the call starts.", (req("x"),)),
        Step(3, "read() returns the next message, or raises TimeoutError.", (resp("x-0"),)),
        Step(4, "Iteration yields the remaining messages...", (resp("x-1"), resp("x-2"))),
        Step(4, "...and stops when the server ends the stream with OK.", (ctl("status OK", DOWN, RESPONSE),)),
        Step(6, "After the end, read() keeps returning EOF."),
    ]
    make("response_stream", "ResponseStream: unary-stream", code, steps)


def request() -> None:
    code = [
        "with RequestStream(stub.Join) as s:",
        '    s.write(Msg(value="a"))',
        '    s.write(Msg(value="b"))',
        '    s.write(Msg(value="c"))',
        "    s.done_writing()",
        "    reply = s.result(timeout=5)",
    ]
    steps = [
        Step(0, "The call starts in the constructor.", (ctl("open stream", UP),)),
        Step(1, "write() queues the message and returns at once.", (req("a"),)),
        Step(2, "write() queues the message and returns at once.", (req("b"),)),
        Step(3, "write() queues the message and returns at once.", (req("c"),)),
        Step(4, "done_writing() half-closes: the server sees the end.", (ctl("half-close", UP),)),
        Step(5, "result() waits for the single response.", (resp("A,B,C"), ctl("status OK", DOWN, RESPONSE))),
    ]
    make("request_stream", "RequestStream: stream-unary", code, steps)


if __name__ == "__main__":
    bidi()
    response()
    request()
