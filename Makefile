NAME = ryan/airflow
VERSION = 1.10.5

.PHONY: build start push

build: build-version

build-version:
	docker build -t ${NAME}:${VERSION}  .

tag-latest:
	docker tag ${NAME}:${VERSION} ${NAME}:latest

test:
	docker run --rm -it -v `pwd`/plugins\:/usr/local/airflow/plugins  -v `pwd`/config/airflow.cfg\:/usr/local/airflow/airflow.cfg -v `pwd`/opt\:/data/opt  -v `pwd`/dags\:/usr/local/airflow/dags   ${NAME}:${VERSION} /bin/bash
	

push:   build-version tag-latest
	docker push ${NAME}:${VERSION}; docker push ${NAME}:latest
      
