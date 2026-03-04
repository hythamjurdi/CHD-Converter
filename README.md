# CHD Converter

A self-hosted web app for bulk converting PS2 (and other disc-based) ISOs to the CHD format. Runs as a Docker container, designed primarily for Unraid but works anywhere Docker runs.

Built with the assistance of [Claude AI](https://claude.ai) by Anthropic.

---

## What it does

- Converts `.iso`, `.img`, `.bin/.cue`, `.mdf`, `.nrg` files to `.chd` using chdman (MAME)
- Extracts and converts directly from `.7z`, `.zip`, `.rar`, `.tar.gz` archives
- Scan your source folder and pick exactly which files to queue before starting
- Real-time progress, live logs, and status per job (Extracting → Running → Rezipping)
- Rezip output — optionally compresses the finished `.chd` back into a `.7z` and removes the standalone file
- PS2 game name lookup — identifies games by disc ID (SLUS/SCES) from filename or `SYSTEM.CNF` inside the ISO and renames output accordingly
- Bad dump detection — flags ISOs that are suspiciously small, too large, or optionally computes MD5 for manual Redump verification
- Conflict resolution — ask / skip / overwrite with apply-to-all option
- Conversion history that survives container restarts
- Dark / light mode


- **RA Hasing is still experimental and needs tweaking**
- RetroAchievements hashing support (requires RA API key)
- Optional retroactive scan to hash previously converted games
- Toggleable RA integration in settings

---

## Docker

```bash
docker run -d \
  --name chd-converter \
  -p 9292:9292 \
  -v /your/isos:/source \
  -v /your/chd:/destination \
  -v /your/appdata/chd-converter:/config \
  hythamjurdi/chd-converter:latest
```

Then open `http://localhost:9292` in your browser.

---

## Docker Compose

```yaml
services:
  chd-converter:
    image: hythamjurdi/chd-converter:latest
    container_name: chd-converter
    ports:
      - "9292:9292"
    volumes:
      - /your/isos:/source
      - /your/chd:/destination
      - /your/appdata/chd-converter:/config
    restart: unless-stopped
```

---

## Unraid

Pending approval in **Community Applications**

Map your source folder (ISOs/archives), destination folder (CHD output), and an appdata path for config. The web UI will be available on port 9292.

---

## Custom game database

The built-in database covers ~200 common PS2 titles. To add your own entries, create `/config/ps2_db.json`:

```json
{
  "SLUS-12345": "My Game Name",
  "SCES-54321": "Another Game"
}
```

The app merges this with the built-in database on startup, with your entries taking priority.

---

## Supported input formats

| Format | Notes |
|--------|-------|
| `.iso` | Most common PS2/PS1 format |
| `.img` | Raw disc image |
| `.bin` + `.cue` | Multi-track CD images |
| `.mdf` + `.mds` | Alcohol 120% format |
| `.nrg` | Nero format |
| `.7z` `.zip` `.rar` `.tar.gz` | Archives containing any of the above |

---

## Notes

- Uses `chdman createcd` for all optical disc formats — this is correct for PS2, PS1, PSP, Dreamcast, Saturn, etc.
- `createhd` (hard disk images) can be forced via the CHD Type Override setting if needed
- Settings and conversion history are stored in `/config` and persist across container restarts

## Changelog

### v1.1.0
- Added RetroAchievements hashing support
- Added retroactive scan for previously converted games
- Improved bin/cue error handling
- Improved queue handling for 2,000+ jobs
- General fixes
