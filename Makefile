PYTHON_VERSIONS := 3.8 3.9 3.10 3.11 3.12

.PHONY: test $(addprefix test-,$(PYTHON_VERSIONS)) clean usage

usage:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  test              Run tests against all Python versions ($(PYTHON_VERSIONS))"
	@echo "  test-<version>    Run tests against a specific Python version (e.g. make test-3.12)"
	@echo "  clean             Remove all pony-test Docker images"
	@echo "  usage             Show this help message"

test: $(addprefix test-,$(PYTHON_VERSIONS))

$(addprefix test-,$(PYTHON_VERSIONS)): test-%:
	docker build --build-arg PYTHON_VERSION=$* -t pony-test-$* .
	docker run --rm pony-test-$*

clean:
	@$(foreach v,$(PYTHON_VERSIONS),docker rmi -f pony-test-$(v) 2>/dev/null || true;)
