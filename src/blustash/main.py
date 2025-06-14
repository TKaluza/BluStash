import asyncio
from pathlib import Path
from sqlalchemy.exc import IntegrityError

from db.models import reg, engine
from db.utils import scan_and_store  # Importiere die Funktion aus utils.py

async def main():
    # 1. Beim ersten Lauf: Erzeuge die Tabellen (falls sie noch nicht existieren)
    async with engine.begin() as conn:
        await conn.run_sync(reg.metadata.create_all)
    print("Tabellen bereit.")

    # 2. Scanne und trage alles ab gewünschtem Root ein.
    basis_pfad = Path("/home/tim/Documents")  # <-- Hier den echten Startpfad eintragen!
    try:
        await scan_and_store(basis_pfad)
    except IntegrityError as e:
        print(f"Integritätsfehler (vermutlich doppelte Einträge?): {e}")
    except Exception as e:
        print(f"Allgemeiner Fehler: {e}")

    print("Scan abgeschlossen, Datenbank ist aktuell.")

if __name__ == "__main__":
    asyncio.run(main())