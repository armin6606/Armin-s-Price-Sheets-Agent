# Price Sheet Bot

Automated price sheet generator. Watches Google Drive for new release PDFs, updates Word templates, and uploads finished DOCX + PDF files back to Drive.

## Project Structure

```
C:\Price Sheets Agent\
  main.py              # CLI entry point
  config.yaml          # Your settings (fill this in!)
  requirements.txt     # Python packages needed
  secrets/             # Put your service_account.json here
  src/                 # Source code modules
  tests/               # Unit tests
  cache/               # Downloaded files, manifests, lock
  logs/                # Log files
```

## Step-by-Step Setup Guide

### Step 1: Install Python

You need Python 3.9 or newer. Open a command prompt and type:

```
python --version
```

If you see `Python 3.9` or higher, you're good. If not, download Python from https://www.python.org/downloads/

### Step 2: Install Required Packages

Open a command prompt, navigate to the project folder, and run:

```
cd "C:\Price Sheets Agent"
pip install -r requirements.txt
```

### Step 3: Set Up Your Service Account JSON

You said you already have a service account. Place your JSON key file at:

```
C:\Price Sheets Agent\secrets\service_account.json
```

### Step 4: Get Your Google Drive Folder IDs

For each folder (Templates, New Releases, Final Price Sheets, SOP):

1. Open the folder in Google Drive in your browser
2. Look at the URL bar: `https://drive.google.com/drive/folders/XXXXXXXXXXXXX`
3. The `XXXXXXXXXXXXX` part is the folder ID
4. Copy it into `config.yaml`

### Step 5: Get Your Spreadsheet ID

1. Open your Google Sheet in the browser
2. Look at the URL: `https://docs.google.com/spreadsheets/d/XXXXXXXXXXXXX/edit`
3. Copy the `XXXXXXXXXXXXX` part into `config.yaml` under `google.spreadsheet_id`

### Step 6: Share Everything with the Service Account

Your service account has an email address (looks like `something@project.iam.gserviceaccount.com`).

You MUST share these with that email:
- Your Google Sheet (Editor access)
- Templates folder (Editor access)
- New Releases folder (Viewer access is enough)
- Final Price Sheets folder (Editor access)
- SOP folder (Viewer access is enough)

To share: Right-click the item in Drive > Share > Add the service account email.

### Step 7: Set Up Your Google Sheet

Your Google Sheet needs TWO tabs:

**CONTROL tab** (columns, case doesn't matter):
| enabled | community | homesite | floorplan | price | address | ready_by | notes |
|---------|-----------|----------|-----------|-------|---------|----------|-------|
| TRUE | Isla | 101 | 2 | 850000 | 123 Oak St | 12/27/2026 | Corner lot |

**MAPPING tab** (columns):
| community | floorplan | file_name | invisible_code |
|-----------|-----------|-----------|----------------|
| Isla | 2 | ISLA_PLAN2.docx | [[PS\|ISLA2]] |

### Step 8: Prepare Your Word Templates

Each Word template (.docx) must have:
1. A table with an invisible code string (like `[[PS|ISLA2]]`) somewhere in a cell
2. Row 2 of that table must be headers: Site, Price, Address, Ready-By, Notes
3. Row 1 can be a title/label row
4. Empty rows below the headers for data to be filled in

Upload templates to your Drive "Templates" folder.

### Step 9: Fill In config.yaml

Open `config.yaml` in a text editor and replace all the `PASTE_YOUR_...` values with your actual IDs.

### Step 10: Run Health Check

```
python main.py --config config.yaml --health-check
```

This checks everything is connected. Fix any issues it reports.

### Step 11: Certify Your Templates

Before processing, certify each template:

```
python main.py --config config.yaml --certify-template --community Isla --floorplan 2
```

This runs 8 checks to make sure the template is ready.

### Step 12: Process New Releases

Drop PDF files into your "New Releases" Google Drive folder. Name them like:
`Community_Homesite_Floorplan.pdf` (e.g., `Isla_101_2.pdf`)

Then run:

```
python main.py --config config.yaml --process-new-releases
```

## All CLI Commands

| Command | What it does |
|---------|-------------|
| `--health-check` | Checks all connections work |
| `--process-new-releases` | Main command: process PDFs |
| `--list-new-releases` | Shows PDFs in New Releases folder |
| `--certify-template --community X --floorplan Y` | Certify a template |
| `--inspect-template-drive --community X --floorplan Y` | Debug a template |
| `--scan-template-drive --file_name FILE.docx` | Find markers in template |
| `--sync-drive-folders` | Verify and cache folder IDs |
| `--audit-report` | Show processing history |
| `--force-lock-reset` | Clear stuck process lock |

### Filters and Overrides

```
--community Isla          # Only process this community
--homesite 101            # Only process this homesite
--floorplan 2             # Only process this floorplan
--dry-run                 # Simulate without uploading
--once                    # Run one cycle only (don't poll)
--overwrite-existing      # Overwrite existing site rows
```

### Polling Mode

Set `app.poll_interval_seconds` to a number > 0 in config.yaml (e.g., 60 for every minute).
The bot will print "Watching for new changes..." and keep running.

## Template Checklist

Before using a template, verify:
- [ ] Template .docx is uploaded to the "Templates" folder on Drive
- [ ] MAPPING tab has a row for this (community, floorplan)
- [ ] invisible_code is placed inside one cell of the target table
- [ ] Row 2 of the target table has headers: Site, Price, Address, Ready-By, Notes
- [ ] There are empty rows below the headers for data
- [ ] Template is certified via `--certify-template`

## Troubleshooting

**"Config file not found"** - Make sure config.yaml is in the same folder as main.py, or use `--config path/to/config.yaml`

**"spreadsheet_id is not set"** - Open config.yaml and paste your real spreadsheet ID

**"folder NOT accessible"** - Share the folder with your service account email

**"Template not found in Drive"** - Check that file_name in MAPPING exactly matches the filename in Drive

**"Invisible code NOT FOUND"** - The code string in MAPPING must exactly match what's in the template cell

**"Process lock held"** - Another instance is running, or it crashed. Use `--force-lock-reset`

## Running Tests

```
cd "C:\Price Sheets Agent"
python -m pytest tests/ -v
```
