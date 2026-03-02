# Setup do ambiente
setup:
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

# Rodar pipeline
run:
	. venv/bin/activate && python main.py

# Rodar testes
test:
	. venv/bin/activate && pytest

# Limpar caches
clean:
	rm -rf __pycache__ src/__pycache__ */__pycache__