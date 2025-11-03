
import os
import sys
import subprocess
from pathlib import Path

def main():
    here = Path(__file__).parent.resolve()
    ui = here / "frontend" / "ui_streamlit.py"
    if not ui.exists():
        print("Cannot find frontend/ui_streamlit.py", file=sys.stderr)
        sys.exit(1)

    env = os.environ.copy()
    cmd = [sys.executable, "-m", "streamlit", "run", str(ui)]
    try:
        subprocess.run(cmd, cwd=str(here), check=True, env=env)
    except subprocess.CalledProcessError as e:
        print(f"Failed to start Streamlit: {e}", file=sys.stderr)
        sys.exit(e.returncode)

if __name__ == "__main__":
    main()
