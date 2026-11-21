.PHONY: test compile lint check clean demo

test:
	python3 -m unittest discover -s tests -v

compile:
	python3 -m compileall -q mesh tests tools

lint:
	python3 tools/check_stdlib_only.py

# CI 里跑的就是这一串
check: compile lint test

# 一台机器上自己跟自己玩一遍：起节点、起控制面、推计划、看结果
demo:
	@python3 tools/demo.py

clean:
	rm -rf build dist *.egg-info .pytest_cache .mesh demo-work
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '*.db*' -delete
