.PHONY: test  # to be able to call task test
curr_dir = $(shell pwd)

test-up:
	docker-compose -f docker-compose.test.yml up -d

test-down:
	docker-compose -f docker-compose.test.yml down --volumes

test:
	docker-compose -f docker-compose.test.yml run --rm app-test pytest ${args}

black:
	docker run --rm -it -v $(curr_dir)/test:/tmp/test -v $(curr_dir)/contessa:/tmp/contessa kiwicom/black:19.10b0 black /tmp/test /tmp/contessa
	
bash:
	docker run --rm -it -v $(curr_dir)/contessa:/app/contessa --entrypoint=/bin/sh contessa -c bash

build-dist:
	docker run --rm -v $(curr_dir):/app/ contessa python setup.py bdist_wheel
