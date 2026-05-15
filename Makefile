PYTHON_VERSIONS := 3.8 3.9 3.10 3.11 3.12

.PHONY: test $(addprefix test-,$(PYTHON_VERSIONS)) clean

test: $(addprefix test-,$(PYTHON_VERSIONS))

$(addprefix test-,$(PYTHON_VERSIONS)): test-%:
	docker build --build-arg PYTHON_VERSION=$* -t pony-test-$* .
	docker run --rm pony-test-$*

clean:
	@$(foreach v,$(PYTHON_VERSIONS),docker rmi -f pony-test-$(v) 2>/dev/null || true;)
