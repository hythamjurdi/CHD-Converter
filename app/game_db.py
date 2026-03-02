import os, re, struct, logging, json

logger = logging.getLogger(__name__)

CUSTOM_DB_PATH = "/config/ps2_db.json"

# Built-in PS2 game database - disc ID -> game name
# Format: REGION-NNNNN (normalized with dash)
BUILTIN_DB = {
    # --- Metal Gear ---
    "SCUS-97010": "Metal Gear Solid 2 Sons of Liberty",
    "SCUS-97072": "Metal Gear Solid 3 Snake Eater",
    "SCUS-97399": "Metal Gear Solid 3 Subsistence",
    # --- Gran Turismo ---
    "SCUS-97102": "Gran Turismo 3 A-Spec",
    "SCUS-97268": "Gran Turismo 4",
    "SCUS-97329": "Gran Turismo Tourist Trophy",
    # --- GTA ---
    "SLUS-20073": "Grand Theft Auto III",
    "SLUS-20001": "Grand Theft Auto Vice City",
    "SLUS-20069": "Grand Theft Auto San Andreas",
    "SLUS-21423": "Grand Theft Auto Liberty City Stories",
    "SLUS-21590": "Grand Theft Auto Vice City Stories",
    # --- God of War ---
    "SCUS-97399": "God of War",
    "SCUS-97481": "God of War II",
    # --- Sony Exclusives ---
    "SCUS-97113": "Shadow of the Colossus",
    "SCUS-97044": "Ico",
    "SCUS-97198": "Jak and Daxter The Precursor Legacy",
    "SCUS-97321": "Jak II",
    "SCUS-97429": "Jak 3",
    "SCUS-97501": "Jak X Combat Racing",
    "SCUS-97198": "Sly Cooper and the Thievius Raccoonus",
    "SCUS-97330": "Sly 2 Band of Thieves",
    "SCUS-97454": "Sly 3 Honor Among Thieves",
    "SCUS-97123": "Ratchet and Clank",
    "SCUS-97268": "Ratchet and Clank Going Commando",
    "SCUS-97353": "Ratchet and Clank Up Your Arsenal",
    "SCUS-97465": "Ratchet Deadlocked",
    "SCUS-97513": "Ratchet and Clank Size Matters",
    "SCUS-97468": "SOCOM US Navy SEALs",
    "SCUS-97506": "SOCOM 3 US Navy SEALs",
    "SCUS-97316": "Killzone",
    # --- Final Fantasy ---
    "SLUS-20100": "Final Fantasy X",
    "SLUS-20441": "Final Fantasy X-2",
    "SLUS-20963": "Final Fantasy XII",
    "SLUS-21442": "Crisis Core Final Fantasy VII",
    # --- Kingdom Hearts ---
    "SLUS-20371": "Kingdom Hearts",
    "SLUS-20771": "Kingdom Hearts II",
    "SLUS-21005": "Kingdom Hearts Re Chain of Memories",
    # --- Dragon Quest ---
    "SLUS-21221": "Dragon Quest VIII Journey of the Cursed King",
    # --- Devil May Cry ---
    "SLUS-20216": "Devil May Cry",
    "SLUS-20683": "Devil May Cry 2",
    "SLUS-20973": "Devil May Cry 3 Dantes Awakening",
    "SLUS-21233": "Devil May Cry 3 Special Edition",
    # --- Tekken ---
    "SLUS-20001": "Tekken Tag Tournament",
    "SLUS-20376": "Tekken 4",
    "SLUS-20973": "Tekken 5",
    "SLUS-21488": "Tekken 5 Dark Resurrection",
    # --- Battlefield ---
    "SLUS-21168": "Battlefield 2 Modern Combat",
    "SLUS-21522": "Battlefield Bad Company",
    # --- Need for Speed ---
    "SLUS-20113": "Need for Speed Hot Pursuit 2",
    "SLUS-20727": "Need for Speed Underground",
    "SLUS-20907": "Need for Speed Underground 2",
    "SLUS-21077": "Need for Speed Most Wanted",
    "SLUS-21326": "Need for Speed Carbon",
    "SLUS-21555": "Need for Speed ProStreet",
    # --- Burnout ---
    "SLUS-20427": "Burnout",
    "SLUS-20689": "Burnout 2 Point of Impact",
    "SLUS-20813": "Burnout 3 Takedown",
    "SLUS-21214": "Burnout Revenge",
    "SLUS-21441": "Burnout Dominator",
    # --- Resident Evil ---
    "SLUS-20764": "Resident Evil Code Veronica X",
    "SLUS-20717": "Resident Evil Outbreak",
    "SLUS-21005": "Resident Evil Outbreak File 2",
    "SLUS-21282": "Resident Evil 4",
    # --- Silent Hill ---
    "SLUS-20228": "Silent Hill 2",
    "SLUS-20572": "Silent Hill 3",
    "SLUS-20930": "Silent Hill 4 The Room",
    "SLUS-21208": "Silent Hill Origins",
    # --- Crash Bandicoot ---
    "SLUS-20441": "Crash Bandicoot The Wrath of Cortex",
    "SLUS-21161": "Crash Twinsanity",
    "SLUS-21431": "Crash Tag Team Racing",
    # --- Spyro ---
    "SLUS-20362": "Spyro Enter the Dragonfly",
    "SLUS-20783": "Spyro A Heros Tail",
    # --- SSX ---
    "SLUS-20092": "SSX",
    "SLUS-20389": "SSX Tricky",
    "SLUS-20731": "SSX 3",
    "SLUS-21082": "SSX On Tour",
    # --- Tony Hawk ---
    "SLUS-20095": "Tony Hawks Pro Skater 3",
    "SLUS-20483": "Tony Hawks Underground",
    "SLUS-20752": "Tony Hawks Underground 2",
    "SLUS-21154": "Tony Hawks American Wasteland",
    "SLUS-21484": "Tony Hawks Project 8",
    # --- Guitar Hero ---
    "SLUS-21120": "Guitar Hero",
    "SLUS-21161": "Guitar Hero II",
    "SLUS-21385": "Guitar Hero III Legends of Rock",
    "SLUS-21671": "Guitar Hero World Tour",
    # --- Prince of Persia ---
    "SLUS-20743": "Prince of Persia The Sands of Time",
    "SLUS-21000": "Prince of Persia Warrior Within",
    "SLUS-21245": "Prince of Persia The Two Thrones",
    # --- Star Wars ---
    "SLUS-20873": "Star Wars Battlefront",
    "SLUS-21240": "Star Wars Battlefront II",
    "SLUS-20678": "Star Wars Knights of the Old Republic",
    # --- Sports ---
    "SLUS-20516": "FIFA Soccer 2003",
    "SLUS-20788": "Pro Evolution Soccer 5",
    "SLUS-21034": "Pro Evolution Soccer 6",
    "SLUS-20517": "Madden NFL 2003",
    "SLUS-20637": "Madden NFL 2004",
    "SLUS-20755": "Madden NFL 2005",
    # --- WWE ---
    "SLUS-20491": "WWE SmackDown Here Comes the Pain",
    "SLUS-20715": "WWE SmackDown vs Raw",
    "SLUS-20927": "WWE SmackDown vs Raw 2006",
    "SLUS-21102": "WWE SmackDown vs Raw 2007",
    "SLUS-21385": "WWE SmackDown vs Raw 2008",
    # --- Misc ---
    "SLUS-20666": "Katamari Damacy",
    "SLUS-21008": "We Love Katamari",
    "SLUS-20691": "Ace Combat 04 Shattered Skies",
    "SLUS-20855": "Ace Combat 5 The Unsung War",
    "SLUS-21108": "Ace Combat Zero The Belkan War",
    "SLUS-20722": "Castlevania Lament of Innocence",
    "SLUS-21038": "Castlevania Curse of Darkness",
    "SLUS-20743": "Tomb Raider Legend",
    "SLUS-20995": "Shadow Hearts Covenant",
    "SLUS-20971": "Bully",
    "SLUS-20724": "The Warriors",
    "SLUS-20764": "Midnight Club 3 DUB Edition",
    "SLUS-20787": "Spider-Man 2",
    "SLUS-21384": "Spider-Man 3",
    # --- PAL Games (SCES/SLES) ---
    "SCES-50211": "Gran Turismo 3 A-Spec",
    "SCES-51719": "Gran Turismo 4",
    "SCES-53440": "God of War",
    "SCES-54171": "God of War II",
    "SCES-52771": "Shadow of the Colossus",
    "SCES-50760": "Ico",
    "SCES-50967": "Jak and Daxter The Precursor Legacy",
    "SCES-51608": "Jak II",
    "SCES-52460": "Jak 3",
    "SCES-50330": "Ratchet and Clank",
    "SCES-51607": "Ratchet and Clank Going Commando",
    "SCES-52456": "Ratchet and Clank Up Your Arsenal",
    "SCES-52456": "Sly Cooper and the Thievius Raccoonus",
    "SCES-51214": "Sly 2 Band of Thieves",
    "SCES-53032": "Sly 3 Honor Among Thieves",
    "SCES-50300": "Metal Gear Solid 2 Sons of Liberty",
    "SCES-51828": "Metal Gear Solid 3 Snake Eater",
    "SCES-50420": "Final Fantasy X",
    "SCES-51975": "Final Fantasy XII",
    "SLES-51872": "Need for Speed Underground",
    "SLES-52725": "Need for Speed Underground 2",
    "SLES-53327": "Need for Speed Most Wanted",
    "SLES-51472": "Burnout 3 Takedown",
    "SLES-53204": "Burnout Revenge",
    "SLES-54023": "Guitar Hero II",
    "SLES-52626": "Grand Theft Auto San Andreas",
    "SLES-51617": "Grand Theft Auto Vice City",
    "SLES-50330": "Grand Theft Auto III",
    "SLES-53972": "God of War II",
    # --- Japan (SCPS/SLPM) ---
    "SCPS-15015": "Metal Gear Solid 2 Sons of Liberty",
    "SCPS-15102": "Metal Gear Solid 3 Snake Eater",
    "SCPS-15012": "Gran Turismo 3 A-Spec",
    "SLPM-65491": "Final Fantasy X",
    "SLPM-66244": "Final Fantasy XII",
    "SLPM-65852": "Kingdom Hearts",
    "SLPM-66233": "Kingdom Hearts II",
    "SLPM-65464": "Devil May Cry",
    "SLPM-65786": "Devil May Cry 2",
    "SLPM-66089": "Devil May Cry 3",
    "SLPM-66237": "Shadow of the Colossus",
    "SLPM-66045": "Katamari Damacy",
    "SLPM-66137": "We Love Katamari",
    "SLPM-62384": "Tekken 4",
    "SLPM-66141": "Tekken 5",
}

_custom_db = None

def load_custom_db():
    global _custom_db
    if _custom_db is None:
        if os.path.exists(CUSTOM_DB_PATH):
            try:
                with open(CUSTOM_DB_PATH) as f:
                    _custom_db = json.load(f)
                logger.info(f"Loaded custom PS2 DB: {len(_custom_db)} entries")
            except Exception as e:
                logger.warning(f"Could not load custom PS2 DB: {e}")
                _custom_db = {}
        else:
            _custom_db = {}
    return _custom_db

def normalize_id(disc_id):
    """Normalize a disc ID to REGION-NNNNN format."""
    if not disc_id:
        return None
    disc_id = disc_id.strip().upper()
    # Replace underscore with dash
    disc_id = disc_id.replace('_', '-')
    # Remove version suffixes like .02 or ;1
    disc_id = re.sub(r'[.;]\d+$', '', disc_id)
    # Validate format
    m = re.match(r'(SLUS|SCES|SCUS|SLES|SLPM|SCPS|SLPS|SCED|SLED|SCAJ|SCKA|DTL|PAPX|PBPX|PCPX)[-](\d+)', disc_id)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return disc_id

def extract_disc_id_from_filename(filename):
    """Try to extract PS2 disc ID from a filename."""
    name = os.path.splitext(os.path.basename(filename))[0]
    m = re.search(
        r'\b(SLUS|SCES|SCUS|SLES|SLPM|SCPS|SLPS|SCED|SLED|SCAJ|SCKA|DTL)[-_](\d{4,6})',
        name, re.IGNORECASE
    )
    if m:
        return f"{m.group(1).upper()}-{m.group(2)}"
    return None

def extract_disc_id_from_iso(iso_path):
    """
    Read SYSTEM.CNF from inside a PS2 ISO to get the disc ID.
    Uses raw ISO 9660 parsing — no extra libraries required.
    """
    try:
        with open(iso_path, 'rb') as f:
            # PVD is at sector 16 (sectors are 2048 bytes)
            f.seek(16 * 2048)
            pvd = f.read(2048)

            if pvd[1:6] != b'CD001':
                return None  # Not a valid ISO 9660

            # Root directory record starts at offset 156 in PVD
            root_loc = struct.unpack_from('<I', pvd, 156 + 2)[0]
            root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]

            f.seek(root_loc * 2048)
            dir_data = f.read(min(root_size, 8192))

            pos = 0
            while pos < len(dir_data):
                rec_len = dir_data[pos]
                if rec_len == 0:
                    # Skip to next sector
                    next_sector = ((pos // 2048) + 1) * 2048
                    if next_sector >= len(dir_data):
                        break
                    pos = next_sector
                    continue

                if pos + rec_len > len(dir_data):
                    break

                name_len = dir_data[pos + 32]
                if name_len > 0 and pos + 33 + name_len <= len(dir_data):
                    raw_name = dir_data[pos + 33: pos + 33 + name_len]
                    entry_name = raw_name.decode('ascii', errors='ignore').split(';')[0]

                    if entry_name.upper() == 'SYSTEM.CNF':
                        file_loc = struct.unpack_from('<I', dir_data, pos + 2)[0]
                        file_size = struct.unpack_from('<I', dir_data, pos + 10)[0]
                        f.seek(file_loc * 2048)
                        content = f.read(min(file_size, 1024)).decode('ascii', errors='ignore')

                        # PS2 SYSTEM.CNF format: BOOT2 = cdrom0:\SLUS_20170.02;1
                        m = re.search(
                            r'BOOT2\s*=\s*cdrom0:\\([^;\s]+)',
                            content, re.IGNORECASE
                        )
                        if m:
                            raw = os.path.basename(m.group(1)).replace('\\', '').replace('/', '')
                            return normalize_id(raw)

                pos += rec_len

    except Exception as e:
        logger.debug(f"ISO disc ID read failed for {iso_path}: {e}")
    return None

def get_game_name(filename, iso_path=None):
    """
    Get game name for a file.
    1. Try to extract disc ID from filename
    2. Fall back to reading SYSTEM.CNF from inside the ISO
    3. Look up in database

    Returns: (disc_id, game_name) — either can be None
    """
    disc_id = extract_disc_id_from_filename(filename)

    if not disc_id and iso_path and os.path.exists(iso_path):
        logger.info(f"No disc ID in filename, scanning ISO: {os.path.basename(iso_path)}")
        disc_id = extract_disc_id_from_iso(iso_path)

    if not disc_id:
        return None, None

    normalized = normalize_id(disc_id)
    custom = load_custom_db()

    game_name = custom.get(normalized) or custom.get(disc_id) or \
                BUILTIN_DB.get(normalized) or BUILTIN_DB.get(disc_id)

    logger.info(f"Disc ID: {normalized} → Game: {game_name or 'Unknown'}")
    return normalized, game_name
