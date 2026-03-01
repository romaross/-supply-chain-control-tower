PYTHON := python

.PHONY: run app

run:
	$(PYTHON) -m src.pipeline --config configs/dev.yaml

app:
	streamlit run app/app.py
