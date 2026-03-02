import subprocess
import sys

def run_script(script_name):
    print(f"Executando {script_name}...")
    result = subprocess.run(
        [sys.executable, f"src/{script_name}"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Erro em {script_name}")
        print(result.stderr)
        sys.exit(1)

    print(f"{script_name} executado com sucesso.\n")

if __name__ == "__main__":
    run_script("bronze.py")
    run_script("silver.py")
    run_script("gold.py")

    print("Pipeline finalizado com sucesso.")    