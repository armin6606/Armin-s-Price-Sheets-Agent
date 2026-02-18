"""Price Sheet Bot - Main CLI entry point.

JUST RUN:
    python main.py

That's it! One command does everything automatically.
"""

import argparse
import sys

from src.config import Config
from src.logging_setup import setup_logging
from src.runner import (
    run_master,
    run_process_new_releases,
    run_health_check,
    run_audit_report,
    run_certify_template,
    run_certify_all,
    run_inspect_template,
    run_scan_template,
    run_list_new_releases,
    run_sync_drive_folders,
    release_lock,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="Price Sheet Bot",
        description="Automates price sheet generation from Google Drive PDFs.\n\n"
                    "Just run: python main.py",
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")

    # Commands - default is --run (master command)
    cmds = parser.add_mutually_exclusive_group(required=False)
    cmds.add_argument("--run", action="store_true", default=True,
                       help="Run the full pipeline (default)")
    cmds.add_argument("--health-check", action="store_true", help="Verify all connections")
    cmds.add_argument("--process-new-releases", action="store_true", help="Process PDFs only")
    cmds.add_argument("--list-new-releases", action="store_true", help="List PDFs in New Releases")
    cmds.add_argument("--certify-all", action="store_true", help="Certify ALL templates")
    cmds.add_argument("--certify-template", action="store_true", help="Certify one template")
    cmds.add_argument("--inspect-template-drive", action="store_true", help="Inspect a template")
    cmds.add_argument("--scan-template-drive", action="store_true", help="Scan template for markers")
    cmds.add_argument("--sync-drive-folders", action="store_true", help="Cache folder IDs")
    cmds.add_argument("--audit-report", action="store_true", help="Print audit info")
    cmds.add_argument("--force-lock-reset", action="store_true", help="Clear stuck lock")

    # Filters
    parser.add_argument("--community", default=None, help="Filter by community")
    parser.add_argument("--homesite", default=None, help="Filter by homesite")
    parser.add_argument("--floorplan", default=None, help="Filter by floorplan")

    # Overrides
    parser.add_argument("--dry-run", action="store_true", help="Simulate without uploading")
    parser.add_argument("--once", action="store_true", help="Run one cycle only")
    parser.add_argument("--overwrite-existing", action="store_true", help="Overwrite existing rows")

    # Scan options
    parser.add_argument("--file_name", default=None, help="Template filename (for --scan-template-drive)")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Load config
    try:
        cfg = Config.load(args.config)
    except Exception as e:
        print(f"ERROR loading config: {e}")
        sys.exit(1)

    # Apply CLI overrides
    if args.dry_run:
        cfg.app.dry_run = True
    if args.overwrite_existing:
        cfg.app.overwrite_existing = True

    # Setup logging and dirs
    cfg.ensure_cache_dirs()
    cfg.materialize_secrets_from_env()  # Write secrets from env vars (CI/cloud)
    logger = setup_logging()

    # Figure out which command to run
    # (--run is default, but other flags override it)
    if args.health_check:
        ok = run_health_check(cfg)
        sys.exit(0 if ok else 1)

    elif args.process_new_releases:
        run_process_new_releases(
            cfg,
            community_filter=args.community,
            homesite_filter=args.homesite,
            floorplan_filter=args.floorplan,
            overwrite_existing_override=args.overwrite_existing,
            once=args.once,
        )

    elif args.list_new_releases:
        run_list_new_releases(cfg)

    elif args.certify_all:
        ok, total, passed, failed = run_certify_all(cfg)
        print(f"\nDone: {passed}/{total} passed, {failed} failed.")
        sys.exit(0 if ok else 1)

    elif args.certify_template:
        if not args.community or not args.floorplan:
            print("ERROR: --certify-template requires --community and --floorplan")
            sys.exit(1)
        ok = run_certify_template(cfg, args.community, args.floorplan)
        sys.exit(0 if ok else 1)

    elif args.inspect_template_drive:
        if not args.community or not args.floorplan:
            print("ERROR: --inspect-template-drive requires --community and --floorplan")
            sys.exit(1)
        run_inspect_template(cfg, args.community, args.floorplan)

    elif args.scan_template_drive:
        if not args.file_name:
            print("ERROR: --scan-template-drive requires --file_name")
            sys.exit(1)
        run_scan_template(cfg, args.file_name)

    elif args.sync_drive_folders:
        run_sync_drive_folders(cfg)

    elif args.audit_report:
        run_audit_report(cfg)

    elif args.force_lock_reset:
        release_lock()
        print("Process lock cleared.")

    else:
        # Default: run the full pipeline
        run_master(
            cfg,
            community_filter=args.community,
            homesite_filter=args.homesite,
            floorplan_filter=args.floorplan,
            overwrite_existing_override=args.overwrite_existing,
            once=args.once,
        )


if __name__ == "__main__":
    main()
