build-NwsCaptureFunction:
	mkdir -p $(ARTIFACTS_DIR)/pipeline/config \
	         $(ARTIFACTS_DIR)/pipeline/storage \
	         $(ARTIFACTS_DIR)/pipeline/weather
	cp pipeline/__init__.py                 $(ARTIFACTS_DIR)/pipeline/
	cp pipeline/logging_config.py           $(ARTIFACTS_DIR)/pipeline/
	cp pipeline/config/object_store_config.py $(ARTIFACTS_DIR)/pipeline/config/
	cp pipeline/storage/__init__.py         $(ARTIFACTS_DIR)/pipeline/storage/
	cp pipeline/storage/object_store.py     $(ARTIFACTS_DIR)/pipeline/storage/
	cp pipeline/weather/models.py           $(ARTIFACTS_DIR)/pipeline/weather/
	cp pipeline/weather/nws_api_fetcher.py  $(ARTIFACTS_DIR)/pipeline/weather/
	cp pipeline/weather/nws_capture_lambda.py $(ARTIFACTS_DIR)/pipeline/weather/
	pip install --quiet -r requirements.txt --target $(ARTIFACTS_DIR)
