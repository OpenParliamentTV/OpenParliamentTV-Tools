"""Command line of the readiness tooling: ``python -m optv.scripts.readiness <command>``."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from optv.parliaments import list_parliaments

logger = logging.getLogger("optv.scripts.readiness")


def _codes(requested: list[str]) -> list[str]:
    known = list_parliaments()
    unknown = [code for code in requested if code not in known]
    if unknown:
        raise SystemExit(f"unknown parliament: {', '.join(unknown)}")
    return requested or known


def cmd_check(args) -> int:
    from . import overview, scorecard

    cards = []
    for code in _codes(args.codes):
        card = scorecard.evaluate(code)
        if not args.no_write and scorecard.write_if_changed(card):
            logger.info(f"wrote {scorecard.scorecard_path(code)}")
        cards.append(card)
    if not args.no_write and overview.write():
        logger.info("wrote docs/READINESS.md")
    if args.json:
        print(json.dumps(cards, indent=2, ensure_ascii=False))
        return 0
    print(f"{'CODE':<6} {'LEVEL':<15} {'STATUS':<15} OPEN")
    for card in cards:
        notes = []
        if card["blocking"]:
            notes.append("next level: " + ", ".join(card["blocking"]))
        if card["regressions"]:
            notes.append("regressions: " + ", ".join(card["regressions"]))
        if card["unverified"]:
            notes.append("unverified: " + ", ".join(card["unverified"]))
        level = f"{card['level']} {card['level_name']}"
        print(f"{card['parliament']:<6} {level:<15} {card['status']:<15} {'; '.join(notes) or '-'}")
    return 0


def cmd_overview(args) -> int:
    from . import overview

    if args.check:
        if overview.is_current():
            return 0
        logger.error("docs/READINESS.md does not match docs/readiness/*.json; run `check`")
        return 1
    logger.info("wrote docs/READINESS.md" if overview.write() else "docs/READINESS.md is current")
    return 0


def cmd_catalogue(args) -> int:
    from .criteria import CATALOGUE

    for criterion in CATALOGUE:
        print(f"{criterion.id:<24} level {criterion.level}  {criterion.kind:<9} {criterion.rule}")
    return 0


def cmd_qc(args) -> int:
    from . import qc

    (code,) = _codes([args.code])
    try:
        outcome = qc.refresh(code, sessions=args.session, force=args.force)
    except FileNotFoundError as error:
        logger.error(str(error))
        return 2
    for session, result in outcome.items():
        print(f"{code} {session}: {result}")
    return 0


def cmd_estimate(args) -> int:
    from . import estimate, jobs

    codes = _codes(args.codes)
    if args.backtest:
        for code in codes:
            print(estimate.render_backtest(code, estimate.backtest(code)))
        return 0
    if args.session and len(codes) != 1:
        raise SystemExit("--session needs exactly one parliament")
    try:
        plan = estimate.build_plan(codes, sessions={codes[0]: args.session} if args.session else None)
    except estimate.ScopeError as error:
        logger.error(str(error))
        return 2
    if plan["steps"]:
        jobs.save_plan(plan)
    if args.json:
        print(json.dumps(dict(plan, token=estimate.plan_token(plan)), indent=2, ensure_ascii=False))
    else:
        print(estimate.render_plan(plan))
    return 0


def cmd_start(args) -> int:
    from . import jobs

    try:
        pid = jobs.start(args.job_id, args.confirm)
    except jobs.JobError as error:
        logger.error(str(error))
        return 2
    print(f"job {args.job_id} is running detached (pid {pid}); "
          f"follow it with `python -m optv.scripts.readiness jobs show {args.job_id}`")
    return 0


def cmd_jobs(args) -> int:
    from . import jobs

    try:
        if args.action == "list":
            listing = jobs.list_jobs()
            if args.json:
                print(json.dumps(listing, indent=2, ensure_ascii=False))
            else:
                print(jobs.render_list(listing))
            return 0
        if not args.job_id:
            raise SystemExit(f"`jobs {args.action}` needs a job id")
        if args.action == "show":
            job = jobs.describe(args.job_id)
            print(json.dumps(job, indent=2, ensure_ascii=False) if args.json else jobs.render_job(job))
        elif args.action == "logs":
            print(jobs.tail_logs(args.job_id, lines=args.lines))
        elif args.action == "resume":
            pid = jobs.resume(args.job_id)
            print(f"job {args.job_id} resumed detached (pid {pid})")
        elif args.action == "cancel":
            print(jobs.cancel(args.job_id))
    except jobs.JobError as error:
        logger.error(str(error))
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m optv.scripts.readiness",
        description="Score the parliament implementations and their sample data, and run the "
                    "Whisper transcriptions that evidence needs as detached jobs.")
    parser.add_argument("--debug", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="evaluate the criteria and write the scorecards and the overview")
    check.add_argument("codes", nargs="*", metavar="CODE", help="parliaments; default: all")
    check.add_argument("--all", action="store_true", help="all parliaments (the default)")
    check.add_argument("--no-write", action="store_true", help="evaluate without writing any file")
    check.add_argument("--json", action="store_true", help="print the scorecards as JSON")
    check.set_defaults(func=cmd_check)

    overview = sub.add_parser("overview", help="render docs/READINESS.md from the scorecards")
    overview.add_argument("--check", action="store_true", help="only report whether the file is current")
    overview.set_defaults(func=cmd_overview)

    catalogue = sub.add_parser("catalogue", help="list the criteria")
    catalogue.set_defaults(func=cmd_catalogue)

    qc = sub.add_parser("qc", help="recompute outdated Whisper QC figures of the sample sessions")
    qc.add_argument("code", metavar="CODE")
    qc.add_argument("--session", action="append", help="one sample session; repeatable")
    qc.add_argument("--force", action="store_true", help="recompute current figures too")
    qc.set_defaults(func=cmd_qc)

    estimate = sub.add_parser("estimate", help="plan the outstanding transcriptions and estimate their duration")
    estimate.add_argument("codes", nargs="+", metavar="CODE")
    estimate.add_argument("--session", action="append", help="one sample session of a single parliament; repeatable")
    estimate.add_argument("--json", action="store_true")
    estimate.add_argument("--backtest", action="store_true",
                          help="compare the cost model with the recorded transcription times")
    estimate.set_defaults(func=cmd_estimate)

    start = sub.add_parser("start", help="run an estimated plan as a detached job")
    start.add_argument("job_id", metavar="JOB_ID")
    start.add_argument("--confirm", required=True, metavar="TOKEN",
                       help="the token `estimate` printed for this plan")
    start.set_defaults(func=cmd_start)

    jobs = sub.add_parser("jobs", help="list, inspect, resume or cancel jobs")
    jobs.add_argument("action", nargs="?", default="list",
                      choices=["list", "show", "logs", "resume", "cancel"])
    jobs.add_argument("job_id", nargs="?", metavar="JOB_ID")
    jobs.add_argument("--lines", type=int, default=40, help="log lines to show")
    jobs.add_argument("--json", action="store_true")
    jobs.set_defaults(func=cmd_jobs)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    from optv.shared.cli import setup_logging
    setup_logging(args.debug)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
