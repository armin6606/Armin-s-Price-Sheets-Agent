"""Configuration loader and validator for Price Sheet Bot."""

import json
import os
import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class GoogleConfig:
    spreadsheet_id: str
    credentials_json_path: str
    control_tab: str = "CONTROL"
    mapping_tab: str = "MAPPING"


@dataclass
class DriveConfig:
    enabled: bool = True
    require_folder_ids: bool = True
    allow_name_fallback: bool = False
    shared_drive_id: Optional[str] = None
    templates_folder_id: str = ""
    new_releases_folder_id: str = ""
    final_price_sheets_folder_id: str = ""
    sop_folder_id: str = ""
    allow_deletions: bool = False
    move_processed_pdfs: bool = True
    keep_originals: bool = True
    overwrite_drive_files: bool = False
    download_cache_dir: str = "./cache/downloads"
    folder_cache_file: str = "./cache/drive_folders.json"
    processed_manifest: str = "./cache/processed_manifest.json"


@dataclass
class AppConfig:
    schema_version: int = 1
    strict_mode: bool = True
    poll_interval_seconds: int = 0
    dry_run: bool = False
    skip_unchanged: bool = True
    remove_invisible_code: bool = True
    overwrite_existing: bool = False
    update_only_blank_cells: bool = True
    allow_price_update_when_filling_blanks: bool = False
    allow_uncertified_templates: bool = False
    allow_overwrite_in_production: bool = False
    default_if_blank: str = "leave"
    print_watching_message: bool = True


@dataclass
class PdfConfig:
    enabled: bool = True
    parse_strategy: str = "filename_first"
    require_control_match: bool = True
    on_unknown_pdf: str = "quarantine"
    quarantine_folder_name: str = "Quarantine"


@dataclass
class Config:
    google: GoogleConfig
    drive: DriveConfig
    app: AppConfig
    pdf: PdfConfig
    config_path: str = ""

    @staticmethod
    def load(config_path: str) -> "Config":
        """Load config from YAML file and validate."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        if not raw or not isinstance(raw, dict):
            raise ValueError(f"Config file is empty or invalid: {config_path}")

        # Validate required top-level sections
        for section in ["google", "drive", "app", "pdf"]:
            if section not in raw:
                raise ValueError(f"Missing required config section: '{section}'")

        google_raw = raw["google"]
        drive_raw = raw["drive"]
        app_raw = raw["app"]
        pdf_raw = raw["pdf"]

        # Validate required google fields
        if not google_raw.get("spreadsheet_id") or google_raw["spreadsheet_id"].startswith("PASTE_"):
            raise ValueError(
                "google.spreadsheet_id is not set. "
                "Open your Google Sheet and copy the ID from the URL."
            )
        if not google_raw.get("credentials_json_path"):
            raise ValueError("google.credentials_json_path is required.")

        google = GoogleConfig(
            spreadsheet_id=google_raw["spreadsheet_id"],
            credentials_json_path=google_raw.get("credentials_json_path", "./secrets/service_account.json"),
            control_tab=google_raw.get("control_tab", "CONTROL"),
            mapping_tab=google_raw.get("mapping_tab", "MAPPING"),
        )

        # Validate drive folder IDs if required
        drive = DriveConfig(
            enabled=drive_raw.get("enabled", True),
            require_folder_ids=drive_raw.get("require_folder_ids", True),
            allow_name_fallback=drive_raw.get("allow_name_fallback", False),
            shared_drive_id=drive_raw.get("shared_drive_id"),
            templates_folder_id=drive_raw.get("templates_folder_id", ""),
            new_releases_folder_id=drive_raw.get("new_releases_folder_id", ""),
            final_price_sheets_folder_id=drive_raw.get("final_price_sheets_folder_id", ""),
            sop_folder_id=drive_raw.get("sop_folder_id", ""),
            allow_deletions=drive_raw.get("allow_deletions", False),
            move_processed_pdfs=drive_raw.get("move_processed_pdfs", True),
            keep_originals=drive_raw.get("keep_originals", True),
            overwrite_drive_files=drive_raw.get("overwrite_drive_files", False),
            download_cache_dir=drive_raw.get("download_cache_dir", "./cache/downloads"),
            folder_cache_file=drive_raw.get("folder_cache_file", "./cache/drive_folders.json"),
            processed_manifest=drive_raw.get("processed_manifest", "./cache/processed_manifest.json"),
        )

        if drive.enabled and drive.require_folder_ids:
            for folder_key in ["templates_folder_id", "new_releases_folder_id",
                               "final_price_sheets_folder_id", "sop_folder_id"]:
                val = getattr(drive, folder_key)
                if not val or val.startswith("PASTE_"):
                    raise ValueError(
                        f"drive.{folder_key} is not set. "
                        f"Open the folder in Google Drive and copy the ID from the URL."
                    )

        app = AppConfig(
            schema_version=app_raw.get("schema_version", 1),
            strict_mode=app_raw.get("strict_mode", True),
            poll_interval_seconds=app_raw.get("poll_interval_seconds", 0),
            dry_run=app_raw.get("dry_run", False),
            skip_unchanged=app_raw.get("skip_unchanged", True),
            remove_invisible_code=app_raw.get("remove_invisible_code", True),
            overwrite_existing=app_raw.get("overwrite_existing", False),
            update_only_blank_cells=app_raw.get("update_only_blank_cells", True),
            allow_price_update_when_filling_blanks=app_raw.get("allow_price_update_when_filling_blanks", False),
            allow_uncertified_templates=app_raw.get("allow_uncertified_templates", False),
            allow_overwrite_in_production=app_raw.get("allow_overwrite_in_production", False),
            default_if_blank=app_raw.get("default_if_blank", "leave"),
            print_watching_message=app_raw.get("print_watching_message", True),
        )

        pdf = PdfConfig(
            enabled=pdf_raw.get("enabled", True),
            parse_strategy=pdf_raw.get("parse_strategy", "filename_first"),
            require_control_match=pdf_raw.get("require_control_match", True),
            on_unknown_pdf=pdf_raw.get("on_unknown_pdf", "quarantine"),
            quarantine_folder_name=pdf_raw.get("quarantine_folder_name", "Quarantine"),
        )

        return Config(
            google=google,
            drive=drive,
            app=app,
            pdf=pdf,
            config_path=str(path.resolve()),
        )

    def ensure_cache_dirs(self):
        """Create cache directories if they don't exist."""
        for d in [self.drive.download_cache_dir,
                  os.path.dirname(self.drive.folder_cache_file),
                  os.path.dirname(self.drive.processed_manifest),
                  "./logs"]:
            if d:
                os.makedirs(d, exist_ok=True)

    def materialize_secrets_from_env(self):
        """Write secret files from environment variables (for CI/cloud use).

        Checks for these env vars and writes them to disk if present:
          - SERVICE_ACCOUNT_JSON  -> secrets/service_account.json
          - OAUTH_CREDENTIALS_JSON -> secrets/oauth_credentials.json
          - OAUTH_USER_TOKEN_JSON  -> secrets/user_token.json
        """
        os.makedirs("./secrets", exist_ok=True)

        env_to_file = {
            "SERVICE_ACCOUNT_JSON": self.google.credentials_json_path,
            "OAUTH_CREDENTIALS_JSON": "./secrets/oauth_credentials.json",
            "OAUTH_USER_TOKEN_JSON": "./secrets/user_token.json",
        }
        for env_var, file_path in env_to_file.items():
            value = os.environ.get(env_var)
            if value:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(value)
                # Validate it's proper JSON
                try:
                    json.loads(value)
                except json.JSONDecodeError:
                    raise ValueError(
                        f"Env var {env_var} is not valid JSON."
                    )
