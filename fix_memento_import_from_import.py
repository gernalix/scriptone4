\
# v1
import re
import sys
from pathlib import Path

def patch_file(path: Path) -> bool:
    txt = path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r'^\s*from\s+memento_sdk\s+import\s*(\((?:.|\n)*?\)|[^\n]+)\s*$', txt, flags=re.M)
    if not m:
        print("[INFO] Nessuna riga 'from memento_sdk import ...' trovata. Nulla da fare.")
        return False

    import_block = m.group(0)
    inner = m.group(1)

    # Normalize to a list of names
    if inner.strip().startswith("("):
        inside = inner.strip()[1:-1]
    else:
        inside = inner.strip()

    # Remove comments and newlines
    inside = re.sub(r'#.*', '', inside)
    parts = [p.strip() for p in re.split(r'[,\n]+', inside) if p.strip()]
    # Handle "name as alias"
    names = []
    assigns = []
    for p in parts:
        if " as " in p:
            name, alias = [x.strip() for x in p.split(" as ", 1)]
        else:
            name, alias = p, p
        names.append((name, alias))

    repl_lines = ["import memento_sdk as _memento_sdk  # patched to avoid circular from-import"]
    for name, alias in names:
        repl_lines.append(f"{alias} = getattr(_memento_sdk, {name!r})")
    repl = "\n".join(repl_lines)

    txt2 = txt.replace(import_block, repl, 1)
    path.write_text(txt2, encoding="utf-8")
    print(f"[OK] Patch applicata a: {path}")
    print("[OK] Ora elimina __pycache__ e rilancia lo script.")
    return True

def main():
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("memento_import.py")
    if target.is_dir():
        target = target / "memento_import.py"
    if not target.exists():
        print(f"[ERRORE] File non trovato: {target}")
        sys.exit(2)
    patched = patch_file(target)
    sys.exit(0 if patched else 0)

if __name__ == "__main__":
    main()
