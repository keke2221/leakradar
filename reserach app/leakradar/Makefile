.PHONY: install run streamlit tests collectors compute lint

install:
	python -m pip install -r requirements.txt

run:
	python run_all.py

streamlit:
	streamlit run app/streamlit_app.py

tests:
	pytest

collectors:
	python scripts/run_collectors.py

compute:
	python scripts/run_compute.py
