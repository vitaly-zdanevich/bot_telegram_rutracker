#!/usr/bin/env python3
import re
import sys
import time


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: since-to-start-time-ms.py SINCE", file=sys.stderr)
        return 2

    value = sys.argv[1].strip()
    match = re.fullmatch(r"(\d+)([smhd]?)", value)
    if not match:
        print("SINCE must look like 30m, 1h, 2d, or raw seconds", file=sys.stderr)
        return 2

    amount = int(match.group(1))
    unit = match.group(2) or "s"
    multiplier = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
    }[unit]
    print(int((time.time() - amount * multiplier) * 1000))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
